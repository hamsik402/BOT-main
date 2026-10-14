//! Live feed for arb-bot without a Geyser plugin.
//!
//! Follows arbitrage bot wallets over an HTTP RPC to find the PumpSwap / Meteora pools they trade,
//! streams those pools over a WebSocket RPC and republishes every account update on the ZeroMQ
//! sockets arb-bot reads (format of `utils::deserialize::deserialize_v2`).
//!
//! feed-bridge <config.toml>          run the bridge (`[feed_bridge]` section)
//! feed-bridge <config.toml> listen   print what is published, to check it without the bot

mod grpc;
mod listen;
mod pools;
mod publisher;
mod rpc;
mod scanner;
mod tracker;
mod ws;

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context};
use log::info;
use solana_program::pubkey::Pubkey;
use tokio::sync::mpsc::unbounded_channel;

use crate::rpc::HttpRpc;

const USAGE: &str = "usage: feed-bridge <config.toml> [listen]";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let mut args = std::env::args().skip(1);
    let config_path = args.next().context(USAGE)?;
    let mode = args.next();
    config::init(&config_path);
    let cfg = config::CONFIG.feed_bridge.clone();
    match mode.as_deref() {
        None => {}
        Some("listen") => return listen::run(&cfg),
        Some(other) => bail!("unknown mode {other:?}, {USAGE}"),
    }
    ensure!(!cfg.rpc.is_empty(), "[feed_bridge] rpc must be set");
    match cfg.source.as_str() {
        "ws" => ensure!(!cfg.ws.is_empty(), "[feed_bridge] ws must be set"),
        "grpc" => ensure!(!cfg.grpc_url.is_empty(), "[feed_bridge] grpc_url must be set"),
        other => bail!("[feed_bridge] source must be \"ws\" or \"grpc\", not {other:?}"),
    }
    info!(
        "Account updates from {}{}",
        cfg.source,
        if cfg.source == "ws" { format!(" ({} endpoint(s))", 1 + cfg.extra_ws.len()) } else { String::new() }
    );

    let rpc = Arc::new(HttpRpc::new("rpc", &cfg.rpc, cfg.rpc_rps));

    let (outgoing_tx, outgoing_rx) = std::sync::mpsc::channel();
    let (subscribers_tx, subscribers_rx) = unbounded_channel();
    publisher::spawn(&cfg.accounts_socket, &cfg.slot_socket, outgoing_rx, subscribers_tx)?;

    let (candidates_tx, candidates_rx) = unbounded_channel();
    if cfg.static_pools.is_empty() {
        let wallets = read_wallets(&cfg.wallets_file)?;
        info!("Following {} bot wallets from {}", wallets.len(), cfg.wallets_file);
        tokio::spawn(scanner::run(
            rpc.clone(),
            wallets,
            cfg.bootstrap_tx_per_wallet,
            Duration::from_millis(cfg.poll_interval_ms),
            candidates_tx,
        ));
    } else {
        let candidates = static_candidates(&rpc, &cfg.static_pools).await?;
        info!("Following {} fixed pools (static_pools), no bot wallet", candidates.len());
        candidates_tx.send(candidates)?;
    }
    tracker::run(cfg, rpc, candidates_rx, subscribers_rx, outgoing_tx).await
}

/// The pools of `[feed_bridge] static_pools`, with their DEX read from the account owner.
async fn static_candidates(rpc: &HttpRpc, addresses: &[String]) -> anyhow::Result<Vec<scanner::Candidate>> {
    let pools = addresses
        .iter()
        .map(|address| Pubkey::from_str(address).with_context(|| format!("static_pools: invalid address {address:?}")))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut candidates = Vec::with_capacity(pools.len());
    for (pool, account, _) in rpc.get_multiple_accounts(&pools).await? {
        let account = account.with_context(|| format!("static_pools: {pool} does not exist"))?;
        let dex = pools::Dex::from_program(&account.owner)
            .with_context(|| format!("static_pools: {pool} is owned by {}, not a PumpSwap / Meteora program", account.owner))?;
        candidates.push(scanner::Candidate {
            pool,
            dex,
            wallet: Pubkey::default(),
            block_time: 0,
        });
    }
    Ok(candidates)
}

/// One address per line; `#` starts a comment.
fn read_wallets(path: &str) -> anyhow::Result<Vec<Pubkey>> {
    let content = std::fs::read_to_string(path).with_context(|| format!("Failed to read {path}"))?;
    let mut wallets = Vec::new();
    for line in content.lines() {
        let address = line.split('#').next().unwrap_or_default().trim();
        if address.is_empty() {
            continue;
        }
        let wallet = Pubkey::from_str(address).with_context(|| format!("{path}: invalid address {address:?}"))?;
        if !wallets.contains(&wallet) {
            wallets.push(wallet);
        }
    }
    ensure!(!wallets.is_empty(), "{path}: no wallet address");
    Ok(wallets)
}
