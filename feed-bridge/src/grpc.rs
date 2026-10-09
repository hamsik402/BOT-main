//! Yellowstone gRPC source, emitting the same events as `ws`. All followed accounts go in one
//! account filter (Shyft allows 10 filters per request but 400k accounts per filter), so DLMM bin
//! arrays are followed one by one instead of with a per-pool program filter.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use anyhow::{bail, Context};
use futures_util::{Sink, SinkExt, StreamExt};
use log::{info, warn};
use solana_program::pubkey::Pubkey;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts,
    SubscribeRequestFilterSlots, SubscribeRequestPing, SubscribeUpdate,
};

use crate::rpc::RawAccount;
use crate::ws::{Sub, WsEvent};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(30);
/// The reconnection delay goes back to its minimum only after a stream that lasted this long, so
/// a server refusing every subscription is not retried in a tight loop.
const STABLE_STREAM: Duration = Duration::from_secs(30);
/// New subscriptions are sent to the server in batches, at most this often.
const UPDATE_INTERVAL: Duration = Duration::from_millis(200);
const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

pub struct GrpcSubscriptions {
    subscribed: BTreeSet<Pubkey>,
    commands: UnboundedSender<Pubkey>,
}

impl GrpcSubscriptions {
    pub fn new(url: &str, x_token: &str, events: UnboundedSender<WsEvent>) -> Self {
        let (commands, rx) = unbounded_channel();
        let x_token = (!x_token.is_empty()).then(|| x_token.to_string());
        tokio::spawn(run(url.to_string(), x_token, rx, events));
        GrpcSubscriptions {
            subscribed: BTreeSet::new(),
            commands,
        }
    }

    pub fn len(&self) -> usize {
        self.subscribed.len()
    }

    /// `DlmmBinArrays` is ignored: the tracker subscribes each bin array account instead.
    pub fn subscribe(&mut self, sub: Sub) {
        if let Sub::Account(pubkey) = sub {
            if self.subscribed.insert(pubkey) {
                let _ = self.commands.send(pubkey);
            }
        }
    }
}

async fn run(url: String, x_token: Option<String>, mut new_accounts: UnboundedReceiver<Pubkey>, events: UnboundedSender<WsEvent>) {
    let mut accounts: BTreeSet<Pubkey> = BTreeSet::new();
    let mut reconnect_delay = Duration::from_secs(1);
    loop {
        while let Ok(pubkey) = new_accounts.try_recv() {
            accounts.insert(pubkey);
        }
        let connected = tokio::time::timeout(CONNECT_TIMEOUT, connect(&url, x_token.clone())).await;
        match connected {
            Ok(Ok(mut client)) => {
                info!("gRPC: connected, {} accounts", accounts.len());
                let started = std::time::Instant::now();
                match serve(&mut client, &mut accounts, &mut new_accounts, &events).await {
                    Ok(()) => return,
                    Err(e) => warn!("gRPC: {e:#}"),
                }
                if started.elapsed() >= STABLE_STREAM {
                    reconnect_delay = Duration::from_secs(1);
                }
            }
            Ok(Err(e)) => warn!("gRPC: connection failed: {e:#}"),
            Err(_) => warn!("gRPC: connection timed out"),
        }
        tokio::time::sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(MAX_RECONNECT_DELAY);
    }
}

async fn connect(
    url: &str,
    x_token: Option<String>,
) -> anyhow::Result<GeyserGrpcClient<impl yellowstone_grpc_client::Interceptor>> {
    let mut builder = GeyserGrpcClient::build_from_shared(url.to_string())?
        .x_token(x_token)?
        .connect_timeout(CONNECT_TIMEOUT)
        .max_decoding_message_size(MAX_MESSAGE_SIZE)
        .tcp_nodelay(true)
        .http2_keep_alive_interval(Duration::from_secs(15))
        .keep_alive_while_idle(true);
    if url.starts_with("https://") {
        builder = builder.tls_config(ClientTlsConfig::new().with_native_roots())?;
    }
    Ok(builder.connect().await?)
}

/// Streams until the connection fails (Err) or the bridge stops (Ok).
async fn serve(
    client: &mut GeyserGrpcClient<impl yellowstone_grpc_client::Interceptor>,
    accounts: &mut BTreeSet<Pubkey>,
    new_accounts: &mut UnboundedReceiver<Pubkey>,
    events: &UnboundedSender<WsEvent>,
) -> anyhow::Result<()> {
    let (mut sink, mut stream) = client
        .subscribe_with_request(Some(subscribe_request(accounts)))
        .await
        .context("subscribe")?;
    if !accounts.is_empty() {
        let _ = events.send(WsEvent::Resubscribed(accounts.iter().map(|pubkey| Sub::Account(*pubkey)).collect()));
    }
    let mut changed = false;
    let mut update_tick = tokio::time::interval(UPDATE_INTERVAL);
    loop {
        tokio::select! {
            pubkey = new_accounts.recv() => {
                let Some(pubkey) = pubkey else { return Ok(()) };
                changed |= accounts.insert(pubkey);
            }
            _ = update_tick.tick(), if changed => {
                // A new request replaces the filters of the stream.
                sink.send(subscribe_request(accounts)).await.context("update subscription")?;
                changed = false;
            }
            message = stream.next() => match message {
                Some(Ok(update)) => {
                    if !handle(update, &mut sink, events).await? {
                        return Ok(());
                    }
                }
                Some(Err(status)) => bail!("stream error: {status}"),
                None => bail!("stream ended"),
            },
        }
    }
}

fn subscribe_request(accounts: &BTreeSet<Pubkey>) -> SubscribeRequest {
    let mut account_filters = HashMap::new();
    if !accounts.is_empty() {
        account_filters.insert(
            "followed".to_string(),
            SubscribeRequestFilterAccounts {
                account: accounts.iter().map(|pubkey| pubkey.to_string()).collect(),
                ..Default::default()
            },
        );
    }
    SubscribeRequest {
        accounts: account_filters,
        slots: HashMap::from([(
            "slots".to_string(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        commitment: Some(CommitmentLevel::Processed as i32),
        ..Default::default()
    }
}

/// Returns false when nobody listens to the events any more.
async fn handle<S>(update: SubscribeUpdate, sink: &mut S, events: &UnboundedSender<WsEvent>) -> anyhow::Result<bool>
where
    S: Sink<SubscribeRequest> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    let event = match update.update_oneof {
        Some(UpdateOneof::Account(update)) => {
            let Some(info) = update.account else { return Ok(true) };
            let (Ok(pubkey), Ok(owner)) = (Pubkey::try_from(info.pubkey.as_slice()), Pubkey::try_from(info.owner.as_slice()))
            else {
                return Ok(true);
            };
            WsEvent::Account {
                pubkey,
                account: RawAccount { owner, data: info.data },
                slot: update.slot,
            }
        }
        Some(UpdateOneof::Slot(slot)) => WsEvent::Slot(slot.slot),
        Some(UpdateOneof::Ping(_)) => {
            // The server closes streams that do not answer its pings.
            sink.send(SubscribeRequest {
                ping: Some(SubscribeRequestPing { id: 1 }),
                ..Default::default()
            })
            .await
            .context("ping reply")?;
            return Ok(true);
        }
        _ => return Ok(true),
    };
    Ok(events.send(event).is_ok())
}
