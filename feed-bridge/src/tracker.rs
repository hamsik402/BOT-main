//! Picks the pools to follow from what the bot wallets trade, keeps their accounts up to date
//! (HTTP snapshot, then WebSocket stream) and publishes every change for arb-bot.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use config::feed_bridge::FeedBridgeConfig;
use log::{info, warn};
use serde_json::{json, Value};
use solana_program::pubkey::Pubkey;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::Instant;

use crate::pools::{dlmm_bin_array_lb_pair, dlmm_bin_arrays_around, Dex, PoolInfo};
use crate::publisher::{encode_accounts, Outgoing};
use crate::rpc::{HttpRpc, RawAccount};
use crate::scanner::{Candidate, TRANSACTIONS_SCANNED};
use crate::grpc::GrpcSubscriptions;
use crate::ws::{Feed, Sub, WsEvent, WsSubscriptions};

/// Updates received within this window and for the same slot are published together, so a pool
/// is recomputed once both of its vaults are updated.
const BATCH_WINDOW: Duration = Duration::from_millis(3);
/// Candidate pools are checked in batches at this interval.
const RESOLVE_INTERVAL: Duration = Duration::from_secs(1);
const STATS_INTERVAL: Duration = Duration::from_secs(60);
/// Pools per ZeroMQ message in a full snapshot.
const KEYFRAME_POOLS_PER_MESSAGE: usize = 50;

struct CachedAccount {
    owner: Pubkey,
    data: Vec<u8>,
    slot: u64,
}

/// A pool seen in the bot transactions.
struct Seen {
    wallets: HashSet<Pubkey>,
    swaps: u64,
    last_seen: i64,
}

/// RPC work, done one job at a time by `run_worker`. DLMM bin arrays are read by address around
/// the active bin (getProgramAccounts is unindexed or limited on most RPCs).
enum Job {
    /// Fetch candidate pools to check them and read their mints.
    Resolve(Vec<(Pubkey, Dex)>),
    /// Fetch `keys` (vaults, bitmap extensions, bin arrays, ...) before following `pools`.
    Activate { pools: Vec<(Pubkey, PoolInfo)>, keys: Vec<Pubkey> },
    /// Fetch accounts again: updates missed during a (re)connection, bin arrays the active bin moved to.
    Refresh(Vec<Pubkey>),
}

impl Job {
    fn keys(&self) -> Vec<Pubkey> {
        match self {
            Job::Resolve(pools) => pools.iter().map(|(pool, _)| *pool).collect(),
            Job::Activate { keys, .. } | Job::Refresh(keys) => keys.clone(),
        }
    }
}

struct Fetched {
    accounts: Vec<(Pubkey, Option<RawAccount>, u64)>,
}

async fn run_worker(rpc: Arc<HttpRpc>, mut jobs: UnboundedReceiver<Job>, done: UnboundedSender<(Job, Fetched)>) {
    while let Some(job) = jobs.recv().await {
        let accounts = rpc.get_multiple_accounts(&job.keys()).await.unwrap_or_else(|e| {
            warn!("RPC: {e:#}");
            Vec::new()
        });
        if done.send((job, Fetched { accounts })).is_err() {
            return;
        }
    }
}

fn bitmap_extension(lb_pair: &Pubkey) -> Pubkey {
    dex::meteora_dlmm::pda::derive_bin_array_bitmap_extension(*lb_pair).0
}

struct Tracker {
    cfg: FeedBridgeConfig,
    accounts: HashMap<Pubkey, CachedAccount>,
    seen: HashMap<Pubkey, Seen>,
    /// Checked pools.
    known: HashMap<Pubkey, PoolInfo>,
    /// Candidates that are not pools of the expected DEX.
    invalid: HashSet<Pubkey>,
    to_resolve: Vec<(Pubkey, Dex)>,
    resolving: HashSet<Pubkey>,
    /// Followed pools.
    active: BTreeSet<Pubkey>,
    /// Account read by the bot for a followed pool (vault, bitmap extension, bin array) -> pool.
    parent: HashMap<Pubkey, Pubkey>,
    dependents: HashMap<Pubkey, BTreeSet<Pubkey>>,
    /// Accounts not tied to one pool (PumpSwap global config).
    globals: BTreeSet<Pubkey>,
    feed: Feed,
    jobs: UnboundedSender<Job>,
    out: Sender<Outgoing>,
    batch: Vec<Pubkey>,
    batch_set: HashSet<Pubkey>,
    batch_slot: u64,
    batch_deadline: Option<Instant>,
    updates: u64,
    messages: u64,
    /// Latest slot of the slot stream, and how many slots account updates arrive behind it.
    latest_slot: u64,
    slot_lag_sum: u64,
    slot_lag_max: u64,
}

pub async fn run(
    cfg: FeedBridgeConfig,
    rpc: Arc<HttpRpc>,
    mut candidates: UnboundedReceiver<Vec<Candidate>>,
    mut subscribers: UnboundedReceiver<()>,
    out: Sender<Outgoing>,
) -> anyhow::Result<()> {
    let (ws_tx, mut ws_rx) = unbounded_channel();
    let (job_tx, job_rx) = unbounded_channel();
    let (done_tx, mut done_rx) = unbounded_channel();
    tokio::spawn(run_worker(rpc, job_rx, done_tx));

    let keyframe_interval = Duration::from_secs(cfg.keyframe_interval_secs);
    let tracker_bin_array_refresh_secs = cfg.bin_array_refresh_secs;
    let mut tracker = Tracker {
        feed: match cfg.source.as_str() {
            "grpc" => Feed::Grpc(GrpcSubscriptions::new(&cfg.grpc_url, &cfg.grpc_token, ws_tx)),
            _ => Feed::Ws(WsSubscriptions::new(&cfg.ws, cfg.max_subscriptions_per_connection, ws_tx)),
        },
        cfg,
        accounts: HashMap::new(),
        seen: HashMap::new(),
        known: HashMap::new(),
        invalid: HashSet::new(),
        to_resolve: Vec::new(),
        resolving: HashSet::new(),
        active: BTreeSet::new(),
        parent: HashMap::new(),
        dependents: HashMap::new(),
        globals: BTreeSet::new(),
        jobs: job_tx,
        out,
        batch: Vec::new(),
        batch_set: HashSet::new(),
        batch_slot: 0,
        batch_deadline: None,
        updates: 0,
        messages: 0,
        latest_slot: 0,
        slot_lag_sum: 0,
        slot_lag_max: 0,
    };

    let bin_array_refresh = Duration::from_secs(tracker_bin_array_refresh_secs);
    let mut bin_array_tick = tokio::time::interval_at(
        Instant::now() + bin_array_refresh.max(Duration::from_secs(1)),
        bin_array_refresh.max(Duration::from_secs(1)),
    );
    let mut resolve_tick = tokio::time::interval(RESOLVE_INTERVAL);
    let mut stats_tick = tokio::time::interval_at(Instant::now() + STATS_INTERVAL, STATS_INTERVAL);
    let mut keyframe_tick = tokio::time::interval_at(
        Instant::now() + keyframe_interval,
        keyframe_interval.max(Duration::from_secs(1)),
    );
    loop {
        let batch_deadline = tracker.batch_deadline;
        tokio::select! {
            Some(event) = ws_rx.recv() => tracker.on_ws_event(event),
            _ = tokio::time::sleep_until(batch_deadline.unwrap_or_else(Instant::now)), if batch_deadline.is_some() => {
                tracker.flush();
            }
            Some(found) = candidates.recv() => tracker.on_candidates(found),
            Some((job, fetched)) = done_rx.recv() => tracker.on_fetched(job, fetched),
            Some(()) = subscribers.recv() => tracker.keyframe(),
            _ = resolve_tick.tick() => tracker.resolve_pending(),
            _ = keyframe_tick.tick(), if !keyframe_interval.is_zero() => tracker.keyframe(),
            _ = stats_tick.tick() => tracker.log_stats(),
            _ = bin_array_tick.tick(), if tracker.feed.follows_bin_arrays_by_address() && !bin_array_refresh.is_zero() => {
                tracker.refresh_bin_arrays();
            }
        }
    }
}

impl Tracker {
    fn on_ws_event(&mut self, event: WsEvent) {
        match event {
            WsEvent::Account { pubkey, account, slot } => {
                self.updates += 1;
                let lag = self.latest_slot.saturating_sub(slot);
                self.slot_lag_sum += lag;
                self.slot_lag_max = self.slot_lag_max.max(lag);
                // programSubscribe also reports bin arrays created after the pool was activated
                if let Some(lb_pair) = dlmm_bin_array_lb_pair(&account) {
                    self.add_dependent(lb_pair, pubkey);
                }
                if !self.store(pubkey, account, slot) {
                    return;
                }
                if !self.batch.is_empty() && slot > self.batch_slot {
                    self.flush();
                }
                self.batch_slot = self.batch_slot.max(slot);
                if self.batch_set.insert(pubkey) {
                    self.batch.push(pubkey);
                }
                if self.batch_deadline.is_none() {
                    self.batch_deadline = Some(Instant::now() + BATCH_WINDOW);
                }
            }
            WsEvent::Slot(slot) => {
                self.latest_slot = self.latest_slot.max(slot);
                let _ = self.out.send(Outgoing::Slot(slot));
            }
            WsEvent::Resubscribed(subs) => {
                info!("Feed connected: fetching {} subscribed accounts again", subs.len());
                let keys = subs
                    .iter()
                    .flat_map(|sub| match sub {
                        Sub::Account(pubkey) => vec![*pubkey],
                        Sub::DlmmBinArrays(lb_pair) => self.bin_arrays_around_active_bin(lb_pair),
                    })
                    .collect();
                let _ = self.jobs.send(Job::Refresh(keys));
            }
        }
    }

    /// Keeps the most recent version of an account. Returns false when `slot` is older than the cached one.
    fn store(&mut self, pubkey: Pubkey, account: RawAccount, slot: u64) -> bool {
        if self.accounts.get(&pubkey).is_some_and(|cached| slot < cached.slot) {
            return false;
        }
        self.accounts.insert(
            pubkey,
            CachedAccount {
                owner: account.owner,
                data: account.data,
                slot,
            },
        );
        true
    }

    fn add_dependent(&mut self, pool: Pubkey, account: Pubkey) {
        if !self.active.contains(&pool) {
            return;
        }
        self.parent.insert(account, pool);
        self.dependents.entry(pool).or_default().insert(account);
    }

    fn dependents_of(&self, pool: &Pubkey) -> impl Iterator<Item = Pubkey> + '_ {
        self.dependents.get(pool).into_iter().flatten().copied()
    }

    /// Publishes the batched updates. Pools come last, with their vaults / bin arrays already
    /// updated, so the bot recomputes each of them once.
    fn flush(&mut self) {
        self.batch_deadline = None;
        self.batch_slot = 0;
        self.batch_set.clear();
        let updated = std::mem::take(&mut self.batch);
        let mut keys = Vec::with_capacity(updated.len() + 2);
        let mut pools = BTreeSet::new();
        for pubkey in updated {
            if self.active.contains(&pubkey) {
                pools.insert(pubkey);
                continue;
            }
            keys.push(pubkey);
            if let Some(pool) = self.parent.get(&pubkey) {
                pools.insert(*pool);
            }
        }
        keys.extend(pools);
        self.publish(&keys);
    }

    fn publish(&mut self, keys: &[Pubkey]) {
        let payload = encode_accounts(keys.iter().filter_map(|key| {
            let account = self.accounts.get(key)?;
            Some((key, &account.owner, account.data.as_slice(), account.slot))
        }));
        if payload.is_empty() {
            return;
        }
        self.messages += 1;
        let _ = self.out.send(Outgoing::Accounts(payload));
    }

    /// Publishes every followed pool with the accounts it depends on, for a new subscriber.
    fn keyframe(&mut self) {
        let pools: Vec<Pubkey> = self.active.iter().copied().collect();
        for chunk in pools.chunks(KEYFRAME_POOLS_PER_MESSAGE) {
            let mut keys: Vec<Pubkey> = self.globals.iter().copied().collect();
            for pool in chunk {
                keys.extend(self.dependents_of(pool));
                keys.push(*pool);
            }
            self.publish(&keys);
        }
    }

    fn on_candidates(&mut self, found: Vec<Candidate>) {
        for candidate in found {
            let seen = self.seen.entry(candidate.pool).or_insert_with(|| Seen {
                wallets: HashSet::new(),
                swaps: 0,
                last_seen: 0,
            });
            seen.swaps += 1;
            seen.wallets.insert(candidate.wallet);
            seen.last_seen = seen.last_seen.max(candidate.block_time);
            if !self.known.contains_key(&candidate.pool)
                && !self.invalid.contains(&candidate.pool)
                && self.resolving.insert(candidate.pool)
            {
                self.to_resolve.push((candidate.pool, candidate.dex));
            }
        }
    }

    fn resolve_pending(&mut self) {
        if !self.to_resolve.is_empty() {
            let _ = self.jobs.send(Job::Resolve(std::mem::take(&mut self.to_resolve)));
        }
    }

    fn on_fetched(&mut self, job: Job, fetched: Fetched) {
        match job {
            Job::Resolve(pools) => {
                let mut accounts: HashMap<Pubkey, (Option<RawAccount>, u64)> = fetched
                    .accounts
                    .into_iter()
                    .map(|(pubkey, account, slot)| (pubkey, (account, slot)))
                    .collect();
                for (pool, dex) in pools {
                    self.resolving.remove(&pool);
                    // Not fetched (RPC error): checked again on its next sighting
                    let Some((account, slot)) = accounts.remove(&pool) else { continue };
                    let info = account.as_ref().and_then(|account| PoolInfo::parse(dex, account));
                    match (info, account) {
                        (Some(info), Some(account)) => {
                            self.store(pool, account, slot);
                            self.known.insert(pool, info);
                        }
                        _ => {
                            self.invalid.insert(pool);
                        }
                    }
                }
                self.select_pools();
            }
            Job::Activate { pools, .. } => {
                self.store_fetched(fetched);
                let global_config = dex::pump_amm::GLOBAL_CONFIG_ID;
                if self.accounts.contains_key(&global_config) && self.globals.insert(global_config) {
                    self.feed.subscribe(Sub::Account(global_config));
                }
                for (pool, info) in pools.iter() {
                    self.feed.subscribe(Sub::Account(*pool));
                    for vault in info.vaults.iter() {
                        self.add_dependent(*pool, *vault);
                        self.feed.subscribe(Sub::Account(*vault));
                    }
                    if info.dex == Dex::MeteoraDlmm {
                        let extension = bitmap_extension(pool);
                        if self.accounts.contains_key(&extension) {
                            self.add_dependent(*pool, extension);
                            self.feed.subscribe(Sub::Account(extension));
                        }
                        // gRPC: bin arrays were subscribed one by one in store_fetched.
                        if !self.feed.follows_bin_arrays_by_address() {
                            self.feed.subscribe(Sub::DlmmBinArrays(*pool));
                        }
                    }
                }
                let mut keys: Vec<Pubkey> = self.globals.iter().copied().collect();
                for (pool, _) in pools.iter() {
                    keys.extend(self.dependents_of(pool));
                    keys.push(*pool);
                }
                self.publish(&keys);
                info!(
                    "Following {} new pools: {} pools, {} subscriptions",
                    pools.len(),
                    self.active.len(),
                    self.feed.len()
                );
                self.write_state();
            }
            Job::Refresh(keys) => {
                // Stored first so that new bin arrays get their pool.
                self.store_fetched(fetched);
                let pools: BTreeSet<Pubkey> = keys
                    .iter()
                    .filter_map(|pubkey| {
                        if self.active.contains(pubkey) {
                            Some(*pubkey)
                        } else {
                            self.parent.get(pubkey).copied()
                        }
                    })
                    .collect();
                let mut keys: Vec<Pubkey> = self.globals.iter().copied().collect();
                for pool in pools.iter() {
                    keys.extend(self.dependents_of(pool));
                    keys.push(*pool);
                }
                self.publish(&keys);
            }
        }
    }

    /// Stores the fetched accounts; bin arrays of followed pools become their dependents.
    fn store_fetched(&mut self, fetched: Fetched) {
        for (pubkey, account, slot) in fetched.accounts {
            let Some(account) = account else { continue };
            if let Some(lb_pair) = dlmm_bin_array_lb_pair(&account) {
                if self.active.contains(&lb_pair) {
                    self.add_dependent(lb_pair, pubkey);
                    if self.feed.follows_bin_arrays_by_address() {
                        self.feed.subscribe(Sub::Account(pubkey));
                    }
                }
            }
            self.store(pubkey, account, slot);
        }
    }

    /// Bin arrays within `bin_array_radius` of the pool's active bin (cached pool account).
    fn bin_arrays_around_active_bin(&self, lb_pair: &Pubkey) -> Vec<Pubkey> {
        let active_id = self
            .accounts
            .get(lb_pair)
            .map(|cached| RawAccount {
                owner: cached.owner,
                data: cached.data.clone(),
            })
            .and_then(|account| PoolInfo::parse(Dex::MeteoraDlmm, &account))
            .and_then(|info| info.dlmm_active_id);
        match active_id {
            Some(active_id) => dlmm_bin_arrays_around(lb_pair, active_id, self.cfg.bin_array_radius),
            None => Vec::new(),
        }
    }

    /// Fetches the bin arrays around the current active bin of every followed DLMM pool, to follow
    /// those the price moved to (the gRPC source cannot discover them by itself).
    fn refresh_bin_arrays(&mut self) {
        let keys: Vec<Pubkey> = self
            .active
            .iter()
            .filter(|pool| self.known.get(*pool).is_some_and(|info| info.dex == Dex::MeteoraDlmm))
            .flat_map(|pool| self.bin_arrays_around_active_bin(pool))
            .filter(|bin_array| !self.parent.contains_key(bin_array))
            .collect();
        if !keys.is_empty() {
            let _ = self.jobs.send(Job::Refresh(keys));
        }
    }

    /// Starts following the checked pools of the tokens matching the selection rule.
    fn select_pools(&mut self) {
        let mut on_pump: HashSet<Pubkey> = HashSet::new();
        let mut on_meteora: HashSet<Pubkey> = HashSet::new();
        for info in self.known.values() {
            let tokens = if info.dex.is_meteora() { &mut on_meteora } else { &mut on_pump };
            tokens.extend(info.tokens().copied());
        }
        let require_both = self.cfg.require_pump_and_meteora;
        let selected: Vec<(Pubkey, PoolInfo)> = self
            .known
            .iter()
            .filter(|(pool, _)| !self.active.contains(*pool))
            .filter(|(_, info)| {
                info.tokens()
                    .any(|token| !require_both || (on_pump.contains(token) && on_meteora.contains(token)))
            })
            .map(|(pool, info)| (*pool, info.clone()))
            .collect();
        if selected.is_empty() {
            return;
        }
        self.active.extend(selected.iter().map(|(pool, _)| *pool));
        let mut keys = vec![dex::pump_amm::GLOBAL_CONFIG_ID];
        for (pool, info) in selected.iter() {
            keys.extend(info.vaults.iter().copied());
            if let Some(active_id) = info.dlmm_active_id {
                keys.push(bitmap_extension(pool));
                keys.extend(dlmm_bin_arrays_around(pool, active_id, self.cfg.bin_array_radius));
            }
        }
        let _ = self.jobs.send(Job::Activate { pools: selected, keys });
    }

    /// Writes the followed tokens and pools to `state_file`.
    fn write_state(&self) {
        let mut tokens: BTreeMap<String, Vec<Value>> = BTreeMap::new();
        for pool in self.active.iter() {
            let Some(info) = self.known.get(pool) else { continue };
            let seen = self.seen.get(pool);
            let entry = json!({
                "pool": pool.to_string(),
                "dex": info.dex.name(),
                "swaps_seen": seen.map_or(0, |seen| seen.swaps),
                "bots": seen.map_or(0, |seen| seen.wallets.len()),
                "last_seen": seen.map_or(0, |seen| seen.last_seen),
            });
            for token in info.tokens() {
                tokens.entry(token.to_string()).or_default().push(entry.clone());
            }
        }
        let updated_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or(0);
        let state = json!({
            "updated_at": updated_at,
            "pools": self.active.len(),
            "token_count": tokens.len(),
            "tokens": tokens,
        });
        let written = serde_json::to_string_pretty(&state)
            .map_err(anyhow::Error::from)
            .and_then(|text| std::fs::write(&self.cfg.state_file, text).map_err(anyhow::Error::from));
        if let Err(e) = written {
            warn!("Failed to write {}: {e:#}", self.cfg.state_file);
        }
    }

    fn log_stats(&mut self) {
        info!(
            "Stats: {} pools followed ({} pools seen, {} rejected), {} subscriptions on {} connections, \
             last minute: {} updates (slot lag avg {:.2}, max {}), {} messages; {} transactions scanned in total",
            self.active.len(),
            self.seen.len(),
            self.invalid.len(),
            self.feed.len(),
            self.feed.connections(),
            self.updates,
            self.slot_lag_sum as f64 / self.updates.max(1) as f64,
            self.slot_lag_max,
            self.messages,
            TRANSACTIONS_SCANNED.load(Ordering::Relaxed),
        );
        self.updates = 0;
        self.messages = 0;
        self.slot_lag_sum = 0;
        self.slot_lag_max = 0;
    }
}
