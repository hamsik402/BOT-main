use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use utils::pool::PoolType;

use crate::DEFAULT_OPTIMIZATION_THREADS;

/// Every DEX family the bot knows about. Used as the default for `dexes`.
pub const ALL_DEXES: [&str; 7] = ["raydium", "pump", "orca", "meteora", "stabble", "saros", "fusion"];

const TX_GROUP_TIME_MICROS: u64 = 700;
const MAX_COUNTER: u64 = 7;

const DEFAULT_VOLUMES: [u64; 30] = [
    500000000000,
    350000000000,
    244999999999,
    171499999999,
    120049999999,
    84034999999,
    58824499999,
    41177149999,
    28824004999,
    20176803499,
    14123762449,
    9886633714,
    6920643600,
    4844450520,
    3391115364,
    2373780754,
    1661646528,
    1163152569,
    814206798,
    569944759,
    398961331,
    279272932,
    195491052,
    136843736,
    95790615,
    67053430,
    46937401,
    32856181,
    22999326,
    16099528,
];

#[derive(Debug, Deserialize, Serialize)]
#[serde(default)]
#[allow(unused)]
pub struct ArbitrageSettings {
    /// Allow two combinations of mints
    pub c2: bool,

    /// Allow three combinations of mints
    pub c3: bool,

    /// Volumes
    pub volumes: Vec<u64>,

    /// Size of the price map
    pub price_map_size: usize,
    //pub price_map_trigger_diff: f64,
    pub optimization_threads: usize,

    // Opportunities
    pub opportunity_filter_factor: u64,
    /// group time in milliseconds
    pub opportunity_group_interval: u64,
    /// max number of calculators in a group
    pub opportunity_group_saturation: u64,

    // flashloan
    pub min_amount_for_flashloan: u64,
    /// price map switch
    pub price_map_tick_switch: u32,

    pub optimize_method: String,

    pub min_wsol: u64,
    //pub min_stable: u64,

    /// Enabled DEXes. Either a family ("raydium", "pump", "orca", "meteora", "stabble", "saros",
    /// "fusion") or a single pool type ("RaydiumClmmV3", "MeteoraDlmm", ...).
    pub dexes: Vec<String>,

    /// PumpSwap <-> Meteora pool selection: only migrated pump.fun tokens whose PumpSwap pool
    /// traded at least this USD volume over 24h (DexScreener), with all their Meteora pools.
    /// 0 disables the selection (every pool of the enabled DEXes is loaded).
    pub pump_min_volume_24h_usd: f64,
    /// Cache file of the pool selection.
    pub pool_selection_cache: String,
    /// The cached selection is rebuilt when older than this, in seconds.
    pub pool_selection_max_age_secs: u64,
    /// No pool is loaded from the RPC at startup: every pool comes from `feed-bridge` over ZeroMQ.
    pub pools_from_feed_bridge: bool,
}

impl ArbitrageSettings {
    #[inline(always)]
    pub fn is_c2_only(&self) -> bool {
        self.c2 && !self.c3
    }

    #[inline(always)]
    pub fn is_c3_only(&self) -> bool {
        self.c3 && !self.c2
    }

    /// Pool types enabled by `dexes`. Panics on an unknown name so a typo never silently disables a DEX.
    pub fn enabled_pool_types(&self) -> HashSet<PoolType> {
        let mut enabled = HashSet::new();
        for name in self.dexes.iter() {
            let pool_types = pool_types_for_dex(name).unwrap_or_else(|| panic!("Unknown DEX in arbitrage.dexes: {name:?}"));
            enabled.extend(pool_types);
        }
        enabled
    }
}

fn pool_types_for_dex(name: &str) -> Option<Vec<PoolType>> {
    let pool_types = match name.to_lowercase().as_str() {
        "raydium" => vec![PoolType::RaydiumAmmV4, PoolType::RaydiumCpmm, PoolType::RaydiumClmmV3],
        "pump" | "pumpfun" | "pump_amm" | "pumpamm" => vec![PoolType::PumpAmm],
        "orca" => vec![PoolType::Orca, PoolType::OrcaSwapV2],
        "meteora" => vec![PoolType::MeteoraDlmm, PoolType::MeteoraDammV2],
        "stabble" => vec![PoolType::StabbleStableSwap, PoolType::StabbleWeightedSwap],
        "saros" => vec![PoolType::SarosAmm, PoolType::SarosDlmm],
        "fusion" => vec![PoolType::FusionAmm],
        "raydiumammv4" => vec![PoolType::RaydiumAmmV4],
        "raydiumcpmm" => vec![PoolType::RaydiumCpmm],
        "raydiumclmmv3" | "raydiumclmm" => vec![PoolType::RaydiumClmmV3],
        "orcawhirlpool" | "whirlpool" => vec![PoolType::Orca],
        "orcaswapv2" => vec![PoolType::OrcaSwapV2],
        "meteoradlmm" => vec![PoolType::MeteoraDlmm],
        "meteoradammv2" => vec![PoolType::MeteoraDammV2],
        "stabblestableswap" => vec![PoolType::StabbleStableSwap],
        "stabbleweightedswap" => vec![PoolType::StabbleWeightedSwap],
        "sarosamm" => vec![PoolType::SarosAmm],
        "sarosdlmm" => vec![PoolType::SarosDlmm],
        "fusionamm" => vec![PoolType::FusionAmm],
        _ => return None,
    };
    Some(pool_types)
}

impl Default for ArbitrageSettings {
    fn default() -> Self {
        ArbitrageSettings {
            c2: true,
            c3: true,
            volumes: DEFAULT_VOLUMES.to_vec(),
            price_map_size: 6,
            optimization_threads: DEFAULT_OPTIMIZATION_THREADS,
            opportunity_filter_factor: 5,
            opportunity_group_interval: TX_GROUP_TIME_MICROS,
            opportunity_group_saturation: MAX_COUNTER,
            min_amount_for_flashloan: 2_000_000_000,
            price_map_tick_switch: 2500,
            optimize_method: "optimize_convex".to_string(),
            min_wsol: 1_000_000_000,
            dexes: ALL_DEXES.iter().map(|dex| dex.to_string()).collect(),
            pump_min_volume_24h_usd: 0.0,
            pool_selection_cache: "pool_selection.json".to_string(),
            pool_selection_max_age_secs: 6 * 3600,
            pools_from_feed_bridge: false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(dexes: &[&str]) -> ArbitrageSettings {
        ArbitrageSettings {
            dexes: dexes.iter().map(|dex| dex.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn default_enables_every_pool_type() {
        assert_eq!(ArbitrageSettings::default().enabled_pool_types().len(), 13);
    }

    #[test]
    fn meteora_and_pump_only() {
        let enabled = settings(&["meteora", "PumpFun"]).enabled_pool_types();
        let expected = HashSet::from([PoolType::MeteoraDlmm, PoolType::MeteoraDammV2, PoolType::PumpAmm]);
        assert_eq!(enabled, expected);
    }

    #[test]
    fn single_pool_type() {
        let enabled = settings(&["RaydiumClmmV3"]).enabled_pool_types();
        assert_eq!(enabled, HashSet::from([PoolType::RaydiumClmmV3]));
    }

    #[test]
    #[should_panic(expected = "Unknown DEX")]
    fn unknown_dex_panics() {
        settings(&["raydum"]).enabled_pool_types();
    }
}
