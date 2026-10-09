//! Pool selection for the PumpSwap <-> Meteora strategy.
//!
//! Keeps only migrated pump.fun tokens whose canonical PumpSwap pool traded at least
//! `pump_min_volume_24h_usd` over the last 24h (DexScreener), together with every Meteora
//! pool (DLMM, DAMM v2) of those tokens. The volume filter applies to PumpSwap only.
//! The result is cached on disk because building it takes several minutes.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use config::arbitrage::ArbitrageSettings;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use utils::constants::WSOL;

const METEORA_DLMM_API: &str = "https://dlmm.datapi.meteora.ag";
const METEORA_DAMM_V2_API: &str = "https://damm-v2.datapi.meteora.ag";
const DEXSCREENER_PAIRS_API: &str = "https://api.dexscreener.com/latest/dex/pairs/solana";

/// Meteora documents 30 RPS for DLMM and 10 RPS for DAMM v2; stay below.
const METEORA_DLMM_INTERVAL: Duration = Duration::from_millis(50);
const METEORA_DAMM_V2_INTERVAL: Duration = Duration::from_millis(125);
const METEORA_PAGE_SIZE: u32 = 1000;

/// DexScreener allows 300 req/min and 30 pairs per request.
const DEXSCREENER_INTERVAL: Duration = Duration::from_millis(250);
const DEXSCREENER_BATCH: usize = 30;

const HTTP_RETRIES: u32 = 6;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectedToken {
    pub mint: String,
    pub pump_pool: String,
    pub pump_volume_24h_usd: f64,
    pub meteora_dlmm_pools: Vec<String>,
    pub meteora_damm_v2_pools: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolSelection {
    /// Unix seconds.
    pub created_at: u64,
    pub pump_min_volume_24h_usd: f64,
    pub tokens: Vec<SelectedToken>,
}

impl PoolSelection {
    pub fn pump_pools(&self) -> Vec<Pubkey> {
        parse_pubkeys(self.tokens.iter().map(|token| &token.pump_pool))
    }

    pub fn meteora_dlmm_pools(&self) -> Vec<Pubkey> {
        parse_pubkeys(self.tokens.iter().flat_map(|token| &token.meteora_dlmm_pools))
    }

    pub fn meteora_damm_v2_pools(&self) -> Vec<Pubkey> {
        parse_pubkeys(self.tokens.iter().flat_map(|token| &token.meteora_damm_v2_pools))
    }

    fn age_secs(&self) -> u64 {
        now_secs().saturating_sub(self.created_at)
    }
}

fn parse_pubkeys<'a>(keys: impl Iterator<Item = &'a String>) -> Vec<Pubkey> {
    let mut pubkeys: Vec<Pubkey> = keys.filter_map(|key| Pubkey::from_str(key).ok()).collect();
    pubkeys.sort_unstable();
    pubkeys.dedup();
    pubkeys
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or_default()
}

/// Canonical PumpSwap pool of a migrated pump.fun token (index 0, quoted in WSOL).
pub fn canonical_pump_pool(mint: &Pubkey) -> Pubkey {
    let (pool_authority, _) =
        Pubkey::find_program_address(&[b"pool-authority", mint.as_ref()], &dex::pump_amm::PUMP_FUN_PROGRAM_ID);
    let (pool, _) = Pubkey::find_program_address(
        &[b"pool", &0u16.to_le_bytes(), pool_authority.as_ref(), mint.as_ref(), WSOL.as_ref()],
        &dex::pump_amm::ID,
    );
    pool
}

/// Returns the cached selection when it is fresh and built with the same threshold, otherwise rebuilds it.
pub async fn load_or_build(settings: &ArbitrageSettings) -> anyhow::Result<PoolSelection> {
    let path = &settings.pool_selection_cache;
    match std::fs::read_to_string(path).map(|text| serde_json::from_str::<PoolSelection>(&text)) {
        Ok(Ok(selection))
            if selection.pump_min_volume_24h_usd == settings.pump_min_volume_24h_usd
                && selection.age_secs() <= settings.pool_selection_max_age_secs =>
        {
            info!(
                "Pool selection: using cache {} ({} tokens, {} s old)",
                path,
                selection.tokens.len(),
                selection.age_secs()
            );
            return Ok(selection);
        }
        Ok(Ok(_)) => info!("Pool selection: cache {} is stale, rebuilding", path),
        Ok(Err(err)) => warn!("Pool selection: invalid cache {}: {:?}, rebuilding", path, err),
        Err(_) => info!("Pool selection: no cache at {}, building", path),
    }

    let selection = build(settings.pump_min_volume_24h_usd).await?;
    if let Err(err) = std::fs::write(path, serde_json::to_string_pretty(&selection)?) {
        warn!("Pool selection: failed to write cache {}: {:?}", path, err);
    }
    Ok(selection)
}

pub async fn build(pump_min_volume_24h_usd: f64) -> anyhow::Result<PoolSelection> {
    let start = Instant::now();
    let client = reqwest::Client::builder().timeout(Duration::from_secs(60)).build()?;

    let dlmm = fetch_meteora_pools(&client, METEORA_DLMM_API, METEORA_DLMM_INTERVAL).await?;
    let damm_v2 = fetch_meteora_pools(&client, METEORA_DAMM_V2_API, METEORA_DAMM_V2_INTERVAL).await?;
    info!(
        "Pool selection: {} Meteora DLMM and {} DAMM v2 pools in {:?}",
        dlmm.len(),
        damm_v2.len(),
        start.elapsed()
    );

    // pump.fun mints end with "pump"; only those can have a canonical PumpSwap pool.
    let mut candidates: BTreeMap<String, SelectedToken> = BTreeMap::new();
    let mut add_pool = |pool: &MeteoraPool, is_dlmm: bool| {
        for mint in [&pool.token_x.address, &pool.token_y.address] {
            if !mint.ends_with("pump") {
                continue;
            }
            let Ok(mint_pubkey) = Pubkey::from_str(mint) else {
                continue;
            };
            let token = candidates.entry(mint.clone()).or_insert_with(|| SelectedToken {
                mint: mint.clone(),
                pump_pool: canonical_pump_pool(&mint_pubkey).to_string(),
                pump_volume_24h_usd: 0.0,
                meteora_dlmm_pools: vec![],
                meteora_damm_v2_pools: vec![],
            });
            if is_dlmm {
                token.meteora_dlmm_pools.push(pool.address.clone());
            } else {
                token.meteora_damm_v2_pools.push(pool.address.clone());
            }
        }
    };
    dlmm.iter().for_each(|pool| add_pool(pool, true));
    damm_v2.iter().for_each(|pool| add_pool(pool, false));
    info!("Pool selection: {} pump.fun tokens with a Meteora pool", candidates.len());

    let pump_pool_to_mint: BTreeMap<String, String> =
        candidates.values().map(|token| (token.pump_pool.clone(), token.mint.clone())).collect();
    let volumes = fetch_pumpswap_volumes(&client, pump_pool_to_mint.keys().cloned().collect()).await?;

    let mut tokens: Vec<SelectedToken> = volumes
        .into_iter()
        .filter(|(_, volume)| *volume >= pump_min_volume_24h_usd)
        .filter_map(|(pump_pool, volume)| {
            let mint = pump_pool_to_mint.get(&pump_pool)?;
            let mut token = candidates.remove(mint)?;
            token.pump_volume_24h_usd = volume;
            Some(token)
        })
        .collect();
    tokens.sort_by(|a, b| b.pump_volume_24h_usd.total_cmp(&a.pump_volume_24h_usd));

    let selection = PoolSelection {
        created_at: now_secs(),
        pump_min_volume_24h_usd,
        tokens,
    };
    info!(
        "Pool selection: {} tokens, {} PumpSwap, {} DLMM, {} DAMM v2 pools (built in {:?})",
        selection.tokens.len(),
        selection.pump_pools().len(),
        selection.meteora_dlmm_pools().len(),
        selection.meteora_damm_v2_pools().len(),
        start.elapsed()
    );
    Ok(selection)
}

#[derive(Debug, Deserialize)]
struct MeteoraToken {
    address: String,
}

#[derive(Debug, Deserialize)]
struct MeteoraPool {
    address: String,
    token_x: MeteoraToken,
    token_y: MeteoraToken,
}

#[derive(Debug, Deserialize)]
struct MeteoraPage {
    pages: u32,
    data: Vec<MeteoraPool>,
}

async fn fetch_meteora_pools(
    client: &reqwest::Client,
    api: &str,
    interval: Duration,
) -> anyhow::Result<Vec<MeteoraPool>> {
    let mut pools = Vec::new();
    let mut page = 1;
    loop {
        let url = format!("{api}/pools?page={page}&page_size={METEORA_PAGE_SIZE}");
        let response: MeteoraPage = get_json(client, &url).await?;
        let pages = response.pages;
        pools.extend(response.data);
        if page >= pages {
            break;
        }
        if page % 20 == 0 {
            info!("Pool selection: {} page {}/{}", api, page, pages);
        }
        page += 1;
        tokio::time::sleep(interval).await;
    }
    Ok(pools)
}

#[derive(Debug, Deserialize)]
struct DexScreenerVolume {
    h24: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DexScreenerPair {
    dex_id: String,
    pair_address: String,
    volume: Option<DexScreenerVolume>,
}

#[derive(Debug, Deserialize)]
struct DexScreenerPairs {
    pairs: Option<Vec<DexScreenerPair>>,
}

/// 24h USD volume of the given PumpSwap pools; pools unknown to DexScreener are omitted.
async fn fetch_pumpswap_volumes(
    client: &reqwest::Client,
    pools: Vec<String>,
) -> anyhow::Result<BTreeMap<String, f64>> {
    let mut volumes = BTreeMap::new();
    let batches = pools.chunks(DEXSCREENER_BATCH).count();
    for (index, batch) in pools.chunks(DEXSCREENER_BATCH).enumerate() {
        let url = format!("{DEXSCREENER_PAIRS_API}/{}", batch.join(","));
        let response: DexScreenerPairs = get_json(client, &url).await?;
        for pair in response.pairs.unwrap_or_default() {
            if pair.dex_id == "pumpswap" {
                let volume = pair.volume.and_then(|volume| volume.h24).unwrap_or_default();
                volumes.insert(pair.pair_address, volume);
            }
        }
        if index % 100 == 0 {
            info!("Pool selection: DexScreener batch {}/{}", index + 1, batches);
        }
        tokio::time::sleep(DEXSCREENER_INTERVAL).await;
    }
    Ok(volumes)
}

async fn get_json<T: serde::de::DeserializeOwned>(client: &reqwest::Client, url: &str) -> anyhow::Result<T> {
    let mut last_error = None;
    for attempt in 0..HTTP_RETRIES {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(2u64.pow(attempt))).await;
        }
        let response = match client.get(url).send().await {
            Ok(response) => response,
            Err(err) => {
                last_error = Some(anyhow::anyhow!("{err:?}"));
                continue;
            }
        };
        let status = response.status();
        if !status.is_success() {
            last_error = Some(anyhow::anyhow!("HTTP {status}"));
            continue;
        }
        let text = response.text().await?;
        return serde_json::from_str(&text).map_err(|err| anyhow::anyhow!("Invalid JSON from {url}: {err}"));
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("No response")).context(format!("GET {url}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_pump_pool_matches_mainnet() {
        // darwin / SOL, see https://dexscreener.com/solana/53W7r4XUxCSnLGoAvj3nPiXnSzyWGZkYHVavYtLpyaXM
        let mint = Pubkey::from_str("5VnbrKp28Qs9CAH6PyZdxBNvLxZcbeX3YBsozgWnpump").unwrap();
        assert_eq!(
            canonical_pump_pool(&mint).to_string(),
            "53W7r4XUxCSnLGoAvj3nPiXnSzyWGZkYHVavYtLpyaXM"
        );
    }
}
