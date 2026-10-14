//! WebSocket subscriptions (accountSubscribe, programSubscribe, slotSubscribe), spread over several
//! connections when one is full, with automatic reconnection. With several WebSocket RPCs, every
//! subscription is made on each of them and the tracker keeps the first copy of each update.

use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use anyhow::bail;
use futures_util::{SinkExt, StreamExt};
use log::{info, warn};
use serde_json::{json, Value};
use solana_program::pubkey::Pubkey;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::pools::dlmm_bin_array_filters;
use crate::rpc::{parse_account, RawAccount};

const KEEPALIVE: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// The reconnection delay goes back to its minimum only after a connection that lasted this long.
const STABLE_CONNECTION: Duration = Duration::from_secs(30);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
/// Request id of slotSubscribe; subscriptions use ids from 1.
const SLOT_REQUEST_ID: u64 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Sub {
    /// accountSubscribe on one account.
    Account(Pubkey),
    /// programSubscribe on Meteora DLMM, filtered on the bin arrays of this pool.
    DlmmBinArrays(Pubkey),
}

#[derive(Debug)]
pub enum WsEvent {
    /// `source`: index of the WebSocket RPC (or 0 for gRPC) the update came from.
    Account {
        pubkey: Pubkey,
        account: RawAccount,
        slot: u64,
        source: usize,
    },
    Slot(u64),
    /// A connection (re)started with these subscriptions: updates may have been missed since they
    /// were requested.
    Resubscribed(Vec<Sub>),
}

/// Connections to one WebSocket RPC.
struct Endpoint {
    url: String,
    /// Number of subscriptions and command channel of each connection.
    connections: Vec<(usize, UnboundedSender<Sub>)>,
}

pub struct WsSubscriptions {
    endpoints: Vec<Endpoint>,
    max_per_connection: usize,
    subscribed: HashSet<Sub>,
    events: UnboundedSender<WsEvent>,
}

impl WsSubscriptions {
    pub fn new(urls: &[String], max_per_connection: usize, events: UnboundedSender<WsEvent>) -> Self {
        WsSubscriptions {
            endpoints: urls
                .iter()
                .map(|url| Endpoint {
                    url: url.clone(),
                    connections: Vec::new(),
                })
                .collect(),
            max_per_connection: max_per_connection.max(1),
            subscribed: HashSet::new(),
            events,
        }
    }

    pub fn len(&self) -> usize {
        self.subscribed.len()
    }

    pub fn connections(&self) -> usize {
        self.endpoints.iter().map(|endpoint| endpoint.connections.len()).sum()
    }

    /// Host of each WebSocket RPC (without the API key), by source index.
    pub fn source_names(&self) -> Vec<String> {
        self.endpoints.iter().map(|endpoint| host(&endpoint.url)).collect()
    }

    pub fn subscribe(&mut self, sub: Sub) {
        if !self.subscribed.insert(sub) {
            return;
        }
        for (source, endpoint) in self.endpoints.iter_mut().enumerate() {
            let index = match endpoint
                .connections
                .iter()
                .position(|(count, _)| *count < self.max_per_connection)
            {
                Some(index) => index,
                None => {
                    let (tx, rx) = unbounded_channel();
                    let index = endpoint.connections.len();
                    // The first connection of each endpoint also streams the slots.
                    let name = format!("{}#{index}", host(&endpoint.url));
                    tokio::spawn(run_connection(endpoint.url.clone(), name, source, index == 0, rx, self.events.clone()));
                    endpoint.connections.push((0, tx));
                    index
                }
            };
            let (count, tx) = &mut endpoint.connections[index];
            *count += 1;
            let _ = tx.send(sub);
        }
    }
}

/// `wss://host/path?api-key=...` -> `host`, for the logs.
fn host(url: &str) -> String {
    let start = url.find("://").map_or(0, |index| index + 3);
    let end = url[start..].find(['/', '?']).map_or(url.len(), |index| start + index);
    url[start..end].to_string()
}

async fn run_connection(
    url: String,
    index: String,
    source: usize,
    with_slots: bool,
    mut new_subs: UnboundedReceiver<Sub>,
    events: UnboundedSender<WsEvent>,
) {
    let mut subs: Vec<Sub> = Vec::new();
    let mut reconnect_delay = Duration::from_secs(1);
    loop {
        while let Ok(sub) = new_subs.try_recv() {
            subs.push(sub);
        }
        match tokio::time::timeout(CONNECT_TIMEOUT, connect_async(with_root_path(&url).as_str())).await {
            Ok(Ok((stream, _))) => {
                info!("WS[{index}]: connected, {} subscriptions", subs.len());
                let started = std::time::Instant::now();
                match serve(stream, source, with_slots, &mut subs, &mut new_subs, &events).await {
                    Ok(()) => return,
                    Err(e) => warn!("WS[{index}]: {e:#}"),
                }
                if started.elapsed() >= STABLE_CONNECTION {
                    reconnect_delay = Duration::from_secs(1);
                }
            }
            Ok(Err(e)) => warn!("WS[{index}]: connection failed: {e}"),
            Err(_) => warn!("WS[{index}]: connection timed out"),
        }
        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
    }
}

/// Subscribes everything in `subs`, then forwards notifications until the connection drops (Err)
/// or the bridge stops (Ok).
async fn serve(
    stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    source: usize,
    with_slots: bool,
    subs: &mut Vec<Sub>,
    new_subs: &mut UnboundedReceiver<Sub>,
    events: &UnboundedSender<WsEvent>,
) -> anyhow::Result<()> {
    let (mut write, mut read) = stream.split();
    let mut next_id: u64 = SLOT_REQUEST_ID + 1;
    // request id -> subscription waiting for its id
    let mut pending: HashMap<u64, Sub> = HashMap::new();
    // subscription id -> subscription
    let mut active: HashMap<u64, Sub> = HashMap::new();

    if with_slots {
        write.send(Message::Text(request(SLOT_REQUEST_ID, "slotSubscribe", json!([])))).await?;
    }
    for sub in subs.iter() {
        write.send(Message::Text(subscribe_request(next_id, sub))).await?;
        pending.insert(next_id, *sub);
        next_id += 1;
    }
    if !subs.is_empty() {
        let _ = events.send(WsEvent::Resubscribed(subs.clone()));
    }

    let mut keepalive = tokio::time::interval(KEEPALIVE);
    keepalive.tick().await;
    loop {
        tokio::select! {
            sub = new_subs.recv() => {
                let Some(sub) = sub else { return Ok(()) };
                write.send(Message::Text(subscribe_request(next_id, &sub))).await?;
                pending.insert(next_id, sub);
                next_id += 1;
                subs.push(sub);
            }
            message = read.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    if !handle_text(&text, source, &mut pending, &mut active, events) {
                        return Ok(());
                    }
                }
                Some(Ok(Message::Close(frame))) => bail!("closed by the server: {frame:?}"),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
                None => bail!("connection ended"),
            },
            _ = keepalive.tick() => write.send(Message::Ping(Vec::new())).await?,
        }
    }
}

/// Returns false when nobody listens to the events any more.
fn handle_text(
    text: &str,
    source: usize,
    pending: &mut HashMap<u64, Sub>,
    active: &mut HashMap<u64, Sub>,
    events: &UnboundedSender<WsEvent>,
) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(text) else { return true };

    // Answer to a subscription request
    if let Some(id) = value.get("id").and_then(Value::as_u64) {
        if let Some(sub) = pending.remove(&id) {
            match value.get("result").and_then(Value::as_u64) {
                Some(subscription) => {
                    active.insert(subscription, sub);
                }
                None => warn!("WS: subscription refused for {sub:?}: {}", value.get("error").unwrap_or(&Value::Null)),
            }
        } else if id == SLOT_REQUEST_ID && value.get("error").is_some() {
            warn!("WS: slotSubscribe refused: {}", value["error"]);
        }
        return true;
    }

    let result = &value["params"]["result"];
    let event = match value["method"].as_str() {
        Some("accountNotification") => {
            let sub = value["params"]["subscription"]
                .as_u64()
                .and_then(|subscription| active.get(&subscription));
            let Some(Sub::Account(pubkey)) = sub else { return true };
            let Some(account) = parse_account(&result["value"]) else { return true };
            WsEvent::Account {
                pubkey: *pubkey,
                account,
                slot: result["context"]["slot"].as_u64().unwrap_or(0),
                source,
            }
        }
        Some("programNotification") => {
            let Some(pubkey) = result["value"]["pubkey"].as_str().and_then(|key| Pubkey::from_str(key).ok()) else {
                return true;
            };
            let Some(account) = parse_account(&result["value"]["account"]) else { return true };
            WsEvent::Account {
                pubkey,
                account,
                slot: result["context"]["slot"].as_u64().unwrap_or(0),
                source,
            }
        }
        Some("slotNotification") => {
            let Some(slot) = result["slot"].as_u64() else { return true };
            WsEvent::Slot(slot)
        }
        _ => return true,
    };
    events.send(event).is_ok()
}

/// `ws://host?query` -> `ws://host/?query`: without a path the handshake request line is invalid
/// and servers answer 400.
fn with_root_path(url: &str) -> String {
    let authority_start = url.find("://").map_or(0, |index| index + 3);
    match url[authority_start..].find(['/', '?']) {
        Some(index) if url.as_bytes()[authority_start + index] == b'?' => {
            format!("{}/{}", &url[..authority_start + index], &url[authority_start + index..])
        }
        _ => url.to_string(),
    }
}

fn request(id: u64, method: &str, params: Value) -> String {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string()
}

fn subscribe_request(id: u64, sub: &Sub) -> String {
    match sub {
        Sub::Account(pubkey) => request(
            id,
            "accountSubscribe",
            json!([pubkey.to_string(), {"encoding": "base64", "commitment": "processed"}]),
        ),
        Sub::DlmmBinArrays(lb_pair) => request(
            id,
            "programSubscribe",
            json!([dex::meteora_dlmm::ID.to_string(), {
                "encoding": "base64",
                "commitment": "processed",
                "filters": dlmm_bin_array_filters(lb_pair),
            }]),
        ),
    }
}

/// Where account updates come from: WebSocket subscriptions or a Yellowstone gRPC stream.
pub enum Feed {
    Ws(WsSubscriptions),
    Grpc(crate::grpc::GrpcSubscriptions),
}

impl Feed {
    pub fn subscribe(&mut self, sub: Sub) {
        match self {
            Feed::Ws(ws) => ws.subscribe(sub),
            Feed::Grpc(grpc) => grpc.subscribe(sub),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Feed::Ws(ws) => ws.len(),
            Feed::Grpc(grpc) => grpc.len(),
        }
    }

    pub fn connections(&self) -> usize {
        match self {
            Feed::Ws(ws) => ws.connections(),
            Feed::Grpc(_) => 1,
        }
    }

    pub fn source_names(&self) -> Vec<String> {
        match self {
            Feed::Ws(ws) => ws.source_names(),
            Feed::Grpc(_) => vec!["grpc".to_string()],
        }
    }

    /// The gRPC source cannot filter bin arrays by pool: each one is subscribed by address.
    pub fn follows_bin_arrays_by_address(&self) -> bool {
        matches!(self, Feed::Grpc(_))
    }
}

#[cfg(test)]
mod tests {
    use super::{host, with_root_path};

    #[test]
    fn host_hides_the_api_key() {
        assert_eq!(host("wss://mainnet.helius-rpc.com/?api-key=k"), "mainnet.helius-rpc.com");
        assert_eq!(host("wss://x.solana-mainnet.quiknode.pro/token/"), "x.solana-mainnet.quiknode.pro");
        assert_eq!(host("ws://host.com:8900"), "host.com:8900");
    }

    #[test]
    fn adds_the_root_path_before_the_query() {
        assert_eq!(with_root_path("ws://host.com?api_key=k"), "ws://host.com/?api_key=k");
        assert_eq!(with_root_path("wss://host.com/?api-key=k"), "wss://host.com/?api-key=k");
        assert_eq!(with_root_path("ws://host.com:8900"), "ws://host.com:8900");
        assert_eq!(with_root_path("ws://host.com/path?x=1"), "ws://host.com/path?x=1");
    }
}
