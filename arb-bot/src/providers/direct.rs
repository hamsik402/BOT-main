//! `direct` provider: arbitrages made only of DEX swap instructions, paid with the wallet's own WSOL
//! (see `arb_core::direct`). Each opportunity is quoted again for `[direct] amount`, simulated, and
//! sent only when the simulation succeeds; the last swap's minimum output makes any transaction
//! that would not end with a profit fail on chain.
//!
//! Transactions go to `send_rpc` and every `send_rpcs` at once, and/or to a Jito block engine as
//! one-transaction bundles (`jito_url`), the Jito tip being the last instruction of the transaction.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ahash::{AHashMap, AHashSet};
use arb_core::arbitrage::OpportunityWithCalculators;
use arb_core::calculator::get_pubkeys;
use arb_core::direct::{build_direct_route, DirectRoute};
use config::CONFIG as cfg;
use jito_searcher_client::tip::get_jito_random_address;
use log::{debug, info, warn};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::hash::Hash;
use solana_sdk::message::{v0, AddressLookupTableAccount, VersionedMessage};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::{Keypair, Signature, Signer};
use solana_sdk::system_instruction;
use solana_sdk::transaction::VersionedTransaction;
use spl_associated_token_account::get_associated_token_address;
use spl_token_2022::extension::transfer_fee::TransferFeeConfig;
use spl_token_2022::extension::{BaseStateWithExtensions, PodStateWithExtensions};
use spl_token_2022::pod::PodMint;
use tokio::select;
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tokio::sync::Mutex;
use tokio::time::Instant;
use utils::constants::{ALLOWED_TOKEN_2022, WSOL};
use utils::transaction::check_transaction_size;

use super::{Provider, SetupResult};
use crate::OptimizeResult;

const STATS_INTERVAL: Duration = Duration::from_secs(60);
const BALANCE_INTERVAL: Duration = Duration::from_secs(5);
/// Sent transactions are checked after this delay.
const STATUS_DELAY: Duration = Duration::from_secs(20);
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
/// Compute units of the SOL transfer tipping Jito.
const JITO_TIP_COMPUTE_UNITS: u32 = 1_000;
/// Jito refuses bundles tipping less than this, in lamports.
const JITO_MIN_TIP: u64 = 1_000;

/// Spaces calls so that at most `per_second` happen per second.
struct RateLimiter {
    interval: Duration,
    next: Mutex<Instant>,
}

impl RateLimiter {
    fn new(per_second: u32) -> Self {
        RateLimiter {
            interval: Duration::from_secs_f64(1.0 / per_second.max(1) as f64),
            next: Mutex::new(Instant::now()),
        }
    }

    async fn wait(&self) {
        let mut next = self.next.lock().await;
        let now = Instant::now();
        if *next > now {
            tokio::time::sleep_until(*next).await;
        }
        *next = (*next).max(now) + self.interval;
    }

    /// Takes a slot only if one is free right now.
    fn try_take(&self) -> bool {
        let Ok(mut next) = self.next.try_lock() else {
            return false;
        };
        let now = Instant::now();
        if *next > now {
            return false;
        }
        *next = now + self.interval;
        true
    }
}

/// Sends transactions to a Jito block engine as one-transaction bundles, over its JSON-RPC API.
struct JitoSender {
    client: reqwest::Client,
    url: String,
}

impl JitoSender {
    fn new(block_engine: &str) -> Self {
        JitoSender {
            client: reqwest::Client::new(),
            url: format!("{}/api/v1/bundles", block_engine.trim_end_matches('/')),
        }
    }

    /// Returns the bundle id.
    async fn send(&self, transaction: &VersionedTransaction) -> anyhow::Result<String> {
        let encoded = utils::base64::b64_encode(check_transaction_size(transaction)?);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [[encoded], {"encoding": "base64"}],
        });
        let response = self
            .client
            .post(&self.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_string())
            .send()
            .await?;
        let status = response.status();
        let text = response.text().await?;
        let value: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();
        match value.get("result").and_then(|result| result.as_str()) {
            Some(bundle_id) => Ok(bundle_id.to_string()),
            None => anyhow::bail!("{status} {}", value.get("error").map(|e| e.to_string()).unwrap_or(text)),
        }
    }
}

/// Token program of a mint, as far as the direct provider is concerned.
#[derive(Clone, Copy, PartialEq, Eq)]
enum MintKind {
    Token,
    Token2022,
    /// Token-2022 with a transfer fee: the calculators ignore it, so its routes are skipped.
    TransferFee,
}

/// Fetches the mints of `mints` missing from `kinds`. Pools only tell the bot which token program
/// a mint uses through their vaults, which DLMM pools are not followed for.
async fn learn_mints(rpc: &RpcClient, limiter: &RateLimiter, mints: &[Pubkey], kinds: &mut AHashMap<Pubkey, MintKind>) {
    let unknown: Vec<Pubkey> = mints.iter().filter(|mint| !kinds.contains_key(*mint)).copied().collect();
    if unknown.is_empty() {
        return;
    }
    limiter.wait().await;
    let accounts = match rpc.get_multiple_accounts(&unknown).await {
        Ok(accounts) => accounts,
        Err(e) => {
            warn!("Direct: mint lookup failed: {e}");
            return;
        }
    };
    for (mint, account) in unknown.iter().zip(accounts) {
        let Some(account) = account else { continue };
        let kind = if account.owner != spl_token_2022::ID {
            MintKind::Token
        } else {
            let fee = PodStateWithExtensions::<PodMint>::unpack(&account.data)
                .ok()
                .and_then(|state| {
                    state.get_extension::<TransferFeeConfig>().ok().map(|config| {
                        u16::from(config.newer_transfer_fee.transfer_fee_basis_points)
                            .max(u16::from(config.older_transfer_fee.transfer_fee_basis_points))
                    })
                })
                .unwrap_or(0);
            if fee > 0 {
                MintKind::TransferFee
            } else {
                MintKind::Token2022
            }
        };
        kinds.insert(*mint, kind);
    }
}

#[derive(Default)]
struct Stats {
    opportunities: u64,
    below_threshold: u64,
    transfer_fee_tokens: u64,
    build_errors: BTreeMap<String, u64>,
    too_large: u64,
    simulated_ok: u64,
    /// Opportunity detection to end of its simulation, in milliseconds.
    latencies_ms: Vec<u64>,
    /// Opportunity detection to the start of its simulation (pipeline + queue), in milliseconds.
    queue_ms: Vec<u64>,
    /// Duration of the simulateTransaction call (rate limiter wait included), in milliseconds.
    simulation_ms: Vec<u64>,
    simulation_errors: BTreeMap<String, u64>,
    sent: u64,
    send_errors: u64,
    jito_sent: u64,
    jito_errors: u64,
    /// Simulated successfully but not sent to Jito because of its rate limit.
    jito_rate_limited: u64,
}

#[derive(Clone, Debug)]
pub struct ProviderDirect {
    pub setup: Option<SetupResult>,
}

impl ProviderDirect {
    pub async fn new() -> ProviderDirect {
        let mut provider = ProviderDirect { setup: None };
        provider.setup = Some(provider.setup().await);
        provider
    }
}

/// Who the transactions are built for: the bot's keypair, or a wallet only simulated.
enum Payer {
    Keypair(Arc<Keypair>),
    SimulatedAs(Pubkey),
}

impl Payer {
    fn pubkey(&self) -> Pubkey {
        match self {
            Payer::Keypair(keypair) => keypair.pubkey(),
            Payer::SimulatedAs(pubkey) => *pubkey,
        }
    }

    fn transaction(&self, message: v0::Message) -> anyhow::Result<VersionedTransaction> {
        match self {
            Payer::Keypair(keypair) => Ok(VersionedTransaction::try_new(
                VersionedMessage::V0(message),
                &[keypair.as_ref()],
            )?),
            Payer::SimulatedAs(_) => Ok(VersionedTransaction {
                signatures: vec![Signature::default(); message.header.num_required_signatures as usize],
                message: VersionedMessage::V0(message),
            }),
        }
    }
}

impl Provider for ProviderDirect {
    fn get_filter(&self) -> u64 {
        1
    }

    fn get_execution_threads(&self) -> u64 {
        1
    }

    fn is_observation_only(&self) -> bool {
        // No flashloan program nor ALT needed.
        true
    }

    fn run(
        self,
        mut rx_arbitrage: UnboundedReceiver<(
            OpportunityWithCalculators,
            OptimizeResult,
            Vec<AddressLookupTableAccount>,
            Vec<AddressLookupTableAccount>,
        )>,
        mut rx_token2022_bc: Receiver<AHashSet<Pubkey>>,
        _cfg_option: Option<String>,
    ) -> Option<tokio::runtime::Runtime> {
        let setup = self.setup.expect("Failed to load setup");
        let direct = cfg.direct.clone();
        let payer = if direct.simulate_as.is_empty() {
            Payer::Keypair(setup.keypair.clone())
        } else {
            Payer::SimulatedAs(Pubkey::from_str(&direct.simulate_as).expect("Invalid [direct] simulate_as"))
        };
        let simulate_only = direct.simulate_only || matches!(payer, Payer::SimulatedAs(_));
        let rpc_url = if direct.rpc.is_empty() { cfg.rpc.clone() } else { direct.rpc.clone() };
        let send_url = if direct.send_rpc.is_empty() { rpc_url.clone() } else { direct.send_rpc.clone() };
        let mut send_urls = vec![send_url];
        for url in direct.send_rpcs.iter().filter(|url| !url.is_empty()) {
            if !send_urls.contains(url) {
                send_urls.push(url.clone());
            }
        }
        let use_jito = !direct.jito_url.is_empty();
        assert!(use_jito || !direct.jito_only, "[direct] jito_only needs jito_url");
        assert!(!use_jito || direct.jito_tip >= JITO_MIN_TIP, "[direct] jito_tip must be at least {JITO_MIN_TIP} lamports");
        let owner = payer.pubkey();
        info!(
            "Direct provider: wallet {owner}, amount {} lamports, min profit {}, {}",
            direct.amount,
            direct.min_profit,
            if simulate_only { "SIMULATION ONLY" } else { "SENDING TRANSACTIONS" }
        );
        info!(
            "Direct provider: sending to {}{}",
            if direct.jito_only { "no RPC".to_string() } else { format!("{} RPC(s)", send_urls.len()) },
            if use_jito {
                format!(", Jito {} (tip {} lamports, {}/s)", direct.jito_url, direct.jito_tip, direct.jito_sends_per_second)
            } else {
                String::new()
            }
        );

        let runtime = tokio::runtime::Runtime::new().expect("Failed to create the direct provider runtime");
        runtime.spawn(async move {
            let rpc = Arc::new(RpcClient::new_with_commitment(rpc_url, CommitmentConfig::processed()));
            let lookup_tables = load_lookup_tables(&rpc, &direct.lookup_tables).await;
            let send_rpcs: Vec<RpcClient> = send_urls
                .into_iter()
                .map(|url| RpcClient::new_with_commitment(url, CommitmentConfig::processed()))
                .collect();
            let jito = use_jito.then(|| JitoSender::new(&direct.jito_url));
            let jito_limiter = RateLimiter::new(direct.jito_sends_per_second);
            let rpc_limiter = Arc::new(RateLimiter::new(direct.rpc_requests_per_second));
            let send_limiter = RateLimiter::new(direct.sends_per_second);
            let stopped = Arc::new(AtomicBool::new(false));
            let wallet_balance = Arc::new(AtomicU64::new(0));

            if !simulate_only {
                tokio::spawn(watch_balance(
                    rpc.clone(),
                    rpc_limiter.clone(),
                    owner,
                    direct.stop_below_balance,
                    wallet_balance.clone(),
                    stopped.clone(),
                ));
            }
            let (tx_sent, rx_sent) = unbounded_channel::<(Signature, i64)>();
            if !simulate_only {
                tokio::spawn(follow_signatures(rpc.clone(), rpc_limiter.clone(), rx_sent));
            }

            let mut blockhash = setup.blockhash;
            let mut rx_blockhash = setup.tx_blockhash.subscribe();
            let mut allowed_token2022 = AHashSet::from_iter(ALLOWED_TOKEN_2022.iter().cloned());
            let mut mint_kinds: AHashMap<Pubkey, MintKind> = AHashMap::new();
            let mut mint_to_ata: ahash::HashMap<Pubkey, Pubkey> = ahash::HashMap::default();
            mint_to_ata.insert(WSOL, get_associated_token_address(&owner, &WSOL));
            let mut stats = Stats::default();
            let mut stats_tick = tokio::time::interval_at(Instant::now() + STATS_INTERVAL, STATS_INTERVAL);

            loop {
                select! {
                    Ok(new_blockhash) = rx_blockhash.recv() => blockhash = new_blockhash,
                    Ok(token_set) = rx_token2022_bc.recv() => {
                        allowed_token2022 = token_set;
                        allowed_token2022.extend(mint_kinds.iter().filter(|(_, kind)| **kind == MintKind::Token2022).map(|(mint, _)| *mint));
                    }
                    _ = stats_tick.tick() => {
                        let percentiles = |values: &mut Vec<u64>| {
                            values.sort_unstable();
                            let at = |p: usize| values.get(values.len() * p / 100).copied().unwrap_or(0);
                            format!("p50 {} ms, p90 {} ms", at(50), at(90))
                        };
                        info!(
                            "Direct latency: {} samples; detection -> simulated {}; of which queue {}, simulation call {}",
                            stats.latencies_ms.len(),
                            percentiles(&mut stats.latencies_ms),
                            percentiles(&mut stats.queue_ms),
                            percentiles(&mut stats.simulation_ms),
                        );
                        info!(
                            "Direct stats (1 min): {} opportunities, {} with transfer fee tokens, {} below {} lamports expected, build errors {:?}, \
                             {} too large, simulations ok {} / errors {:?}, sent {} ({} RPC send errors; Jito {} sent, {} errors, {} rate limited), wallet {} lamports",
                            stats.opportunities, stats.transfer_fee_tokens, stats.below_threshold, direct.min_expected_profit, stats.build_errors,
                            stats.too_large, stats.simulated_ok, stats.simulation_errors, stats.sent, stats.send_errors,
                            stats.jito_sent, stats.jito_errors, stats.jito_rate_limited,
                            wallet_balance.load(Ordering::Relaxed),
                        );
                        stats = Stats::default();
                    }
                    Some((opportunity, _optimize, alts, _)) = rx_arbitrage.recv() => {
                        // Only the latest opportunity of each route is worth trying.
                        let mut batch = vec![(opportunity, alts)];
                        while let Ok((opportunity, _, alts, _)) = rx_arbitrage.try_recv() {
                            batch.push((opportunity, alts));
                        }
                        let mut latest: AHashMap<Vec<Pubkey>, (OpportunityWithCalculators, Vec<AddressLookupTableAccount>)> = AHashMap::new();
                        for (opportunity, alts) in batch {
                            stats.opportunities += 1;
                            latest.insert(get_pubkeys(&opportunity.calculators), (opportunity, alts));
                        }
                        for (_, (opportunity, alts)) in latest {
                            let mint_pair_route = opportunity.mint_pair_route.iter().collect::<Vec<_>>();
                            let mints: Vec<Pubkey> = mint_pair_route.iter().flat_map(|pair| pair.iter()).filter(|mint| **mint != WSOL).copied().collect();
                            learn_mints(&rpc, &rpc_limiter, &mints, &mut mint_kinds).await;
                            if mints.iter().any(|mint| mint_kinds.get(mint) == Some(&MintKind::TransferFee)) {
                                stats.transfer_fee_tokens += 1;
                                continue;
                            }
                            allowed_token2022.extend(mints.iter().filter(|mint| mint_kinds.get(*mint) == Some(&MintKind::Token2022)).copied());
                            let route = match build_direct_route(
                                opportunity.slot,
                                direct.amount,
                                &opportunity.calculators,
                                &mint_pair_route,
                                &WSOL,
                                owner,
                                &mut mint_to_ata,
                                &allowed_token2022,
                                direct.min_profit,
                                direct.wrap_sol,
                                direct.leg_slack_bps,
                            ) {
                                Ok(route) => route,
                                Err(e) => {
                                    *stats.build_errors.entry(error_kind(&e.to_string())).or_default() += 1;
                                    continue;
                                }
                            };
                            if route.expected_profit() < direct.min_expected_profit as i64 {
                                stats.below_threshold += 1;
                                continue;
                            }
                            let mut alts = alts;
                            alts.extend(lookup_tables.iter().cloned());
                            let transaction = match build_transaction(&payer, &route, &alts, blockhash, &direct) {
                                Ok(transaction) => transaction,
                                Err(e) => {
                                    stats.too_large += 1;
                                    debug!("Direct: cannot build the transaction: {e:#}");
                                    continue;
                                }
                            };
                            let pools = get_pubkeys(&opportunity.calculators);
                            let simulation_start = utils::now::as_millis();
                            stats.queue_ms.push(simulation_start.saturating_sub(opportunity.timestamp) as u64);
                            rpc_limiter.wait().await;
                            let simulation = rpc.simulate_transaction_with_config(&transaction, RpcSimulateTransactionConfig {
                                sig_verify: false,
                                replace_recent_blockhash: matches!(payer, Payer::SimulatedAs(_)),
                                commitment: Some(CommitmentConfig::processed()),
                                ..Default::default()
                            }).await;
                            let simulation_end = utils::now::as_millis();
                            stats.simulation_ms.push(simulation_end.saturating_sub(simulation_start) as u64);
                            stats.latencies_ms.push(simulation_end.saturating_sub(opportunity.timestamp) as u64);
                            match simulation.map(|response| response.value) {
                                Ok(result) if result.err.is_none() => {
                                    stats.simulated_ok += 1;
                                    info!(
                                        "Direct: simulation OK, expected profit {} lamports, {} CU, pools {:?}",
                                        route.expected_profit(), result.units_consumed.unwrap_or(0), pools
                                    );
                                }
                                Ok(result) => {
                                    let error = format!("{:?}", result.err);
                                    *stats.simulation_errors.entry(error_kind(&error)).or_default() += 1;
                                    let logs = result.logs.unwrap_or_default();
                                    debug!(
                                        "Direct: simulation failed {error}, expected profit {}, pools {:?}, logs {:?}",
                                        route.expected_profit(), pools, &logs[logs.len().saturating_sub(6)..]
                                    );
                                    continue;
                                }
                                Err(e) => {
                                    *stats.simulation_errors.entry(error_kind(&e.to_string())).or_default() += 1;
                                    continue;
                                }
                            }
                            if simulate_only || stopped.load(Ordering::Relaxed) {
                                continue;
                            }
                            if stats.sent >= direct.max_transactions {
                                warn!("Direct: max_transactions ({}) reached, no more sends", direct.max_transactions);
                                stopped.store(true, Ordering::Relaxed);
                                continue;
                            }
                            let to_jito = jito.is_some() && jito_limiter.try_take();
                            if jito.is_some() && !to_jito {
                                stats.jito_rate_limited += 1;
                            }
                            if direct.jito_only && !to_jito {
                                continue;
                            }
                            let jito_send = async {
                                match &jito {
                                    Some(jito) if to_jito => Some(jito.send(&transaction).await),
                                    _ => None,
                                }
                            };
                            let rpc_send = async {
                                if direct.jito_only {
                                    return Vec::new();
                                }
                                send_limiter.wait().await;
                                futures::future::join_all(send_rpcs.iter().map(|send_rpc| {
                                    send_rpc.send_transaction_with_config(&transaction, RpcSendTransactionConfig {
                                        skip_preflight: true,
                                        ..Default::default()
                                    })
                                }))
                                .await
                            };
                            let (jito_result, rpc_results) = tokio::join!(jito_send, rpc_send);
                            let rpc_ok = rpc_results.iter().filter(|result| result.is_ok()).count();
                            for e in rpc_results.iter().filter_map(|result| result.as_ref().err()) {
                                warn!("Direct: RPC send failed: {e}");
                            }
                            if !rpc_results.is_empty() && rpc_ok == 0 {
                                stats.send_errors += 1;
                            }
                            let jito_ok = match jito_result {
                                Some(Ok(bundle_id)) => {
                                    stats.jito_sent += 1;
                                    debug!("Direct: Jito bundle {bundle_id}");
                                    true
                                }
                                Some(Err(e)) => {
                                    stats.jito_errors += 1;
                                    warn!("Direct: Jito send failed: {e:#}");
                                    false
                                }
                                None => false,
                            };
                            if rpc_ok > 0 || jito_ok {
                                let signature = transaction.signatures[0];
                                stats.sent += 1;
                                info!(
                                    "Direct: SENT {signature} (RPC {rpc_ok}/{}, Jito {}), expected profit {} lamports, pools {:?}",
                                    rpc_results.len(), if jito_ok { "yes" } else { "no" }, route.expected_profit(), pools
                                );
                                let _ = tx_sent.send((signature, route.expected_profit()));
                            }
                        }
                    }
                }
            }
        });
        Some(runtime)
    }
}

fn build_transaction(
    payer: &Payer,
    route: &DirectRoute,
    alts: &[AddressLookupTableAccount],
    blockhash: Hash,
    direct: &config::direct::DirectConfig,
) -> anyhow::Result<VersionedTransaction> {
    let jito_tip = !direct.jito_url.is_empty();
    let route_compute_units = route.compute_units + if jito_tip { JITO_TIP_COMPUTE_UNITS } else { 0 };
    let compute_units = (route_compute_units as u64 * (100 + direct.compute_unit_margin_percent as u64) / 100)
        .min(MAX_COMPUTE_UNITS as u64) as u32;
    let mut instructions = vec![
        ComputeBudgetInstruction::set_compute_unit_limit(compute_units),
        ComputeBudgetInstruction::set_compute_unit_price(direct.compute_unit_price),
    ];
    instructions.extend(route.instructions.iter().cloned());
    if jito_tip {
        // Last instruction: the tip is only paid if every swap before it succeeded.
        instructions.push(system_instruction::transfer(&payer.pubkey(), &get_jito_random_address(), direct.jito_tip));
    }
    let message = v0::Message::try_compile(&payer.pubkey(), &instructions, alts, blockhash)?;
    let transaction = payer.transaction(message)?;
    check_transaction_size(&transaction)?;
    Ok(transaction)
}

async fn load_lookup_tables(rpc: &RpcClient, keys: &[String]) -> Vec<AddressLookupTableAccount> {
    let mut tables = Vec::new();
    for key in keys {
        let Ok(pubkey) = Pubkey::from_str(key) else {
            warn!("Direct: invalid lookup table {key}");
            continue;
        };
        match rpc.get_account_data(&pubkey).await.map_err(anyhow::Error::from).and_then(|data| {
            arb_core::table::get_address_lookup_table(&data, pubkey)
        }) {
            Ok(table) => {
                info!("Direct: lookup table {pubkey} with {} addresses", table.addresses.len());
                tables.push(table);
            }
            Err(e) => warn!("Direct: lookup table {pubkey} not loaded: {e:#}"),
        }
    }
    tables
}

/// Short label grouping similar errors in the stats.
fn error_kind(error: &str) -> String {
    error.chars().take(80).collect()
}

/// Keeps `wallet_balance` (SOL + WSOL) up to date and sets `stopped` below `stop_below`.
async fn watch_balance(
    rpc: Arc<RpcClient>,
    limiter: Arc<RateLimiter>,
    owner: Pubkey,
    stop_below: u64,
    wallet_balance: Arc<AtomicU64>,
    stopped: Arc<AtomicBool>,
) {
    let wsol_ata = get_associated_token_address(&owner, &WSOL);
    let mut first = true;
    loop {
        limiter.wait().await;
        let sol = rpc.get_balance(&owner).await;
        limiter.wait().await;
        let wsol = rpc.get_token_account_balance(&wsol_ata).await;
        // With wrap_sol the WSOL account only exists during a transaction.
        let wsol = wsol.map(|balance| balance.amount.parse::<u64>().unwrap_or(0)).unwrap_or(0);
        match sol {
            Ok(sol) => {
                let total = sol + wsol;
                wallet_balance.store(total, Ordering::Relaxed);
                if first {
                    info!("Direct: wallet balance at start {total} lamports (SOL + WSOL)");
                    first = false;
                }
                if total < stop_below && !stopped.swap(true, Ordering::Relaxed) {
                    warn!("Direct: wallet balance {total} below {stop_below}, sending stopped");
                }
            }
            Err(e) => warn!("Direct: balance check failed: {e}"),
        }
        tokio::time::sleep(BALANCE_INTERVAL).await;
    }
}

/// Logs whether each sent transaction landed, `STATUS_DELAY` after it was sent.
async fn follow_signatures(rpc: Arc<RpcClient>, limiter: Arc<RateLimiter>, mut rx_sent: UnboundedReceiver<(Signature, i64)>) {
    let counters = Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]);
    while let Some((signature, expected_profit)) = rx_sent.recv().await {
        let (rpc, limiter, counters) = (rpc.clone(), limiter.clone(), counters.clone());
        tokio::spawn(async move {
            tokio::time::sleep(STATUS_DELAY).await;
            limiter.wait().await;
            let [landed, failed, dropped] = counters.as_ref();
            match rpc.get_signature_statuses(&[signature]).await {
                Ok(response) => match response.value.into_iter().next().flatten() {
                    Some(status) if status.err.is_none() => {
                        landed.fetch_add(1, Ordering::Relaxed);
                        info!("Direct: LANDED {signature} (expected profit {expected_profit} lamports)");
                    }
                    Some(status) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                        info!("Direct: FAILED on chain {signature}: {:?}", status.err);
                    }
                    None => {
                        dropped.fetch_add(1, Ordering::Relaxed);
                        info!("Direct: NOT LANDED {signature}");
                    }
                },
                Err(e) => warn!("Direct: status of {signature}: {e}"),
            }
            info!(
                "Direct: sent transactions so far: {} landed, {} failed on chain, {} not landed",
                landed.load(Ordering::Relaxed),
                failed.load(Ordering::Relaxed),
                dropped.load(Ordering::Relaxed)
            );
        });
    }
}
