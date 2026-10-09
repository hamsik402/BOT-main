//! What the bridge needs to know about each followed pool type.

use serde_json::{json, Value};
use solana_program::pubkey;
use solana_program::pubkey::Pubkey;

use crate::rpc::RawAccount;

/// Quote mints: a token is identified by its other mint.
pub const QUOTE_MINTS: [Pubkey; 3] = [
    pubkey!("So11111111111111111111111111111111111111112"),  // WSOL
    pubkey!("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"), // USDC
    pubkey!("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"), // USDT
];

/// Offset of `lb_pair` in a DLMM bin array: discriminator (8) + index (8) + version/padding (8).
pub const BIN_ARRAY_LB_PAIR_OFFSET: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Dex {
    PumpAmm,
    MeteoraDlmm,
    MeteoraDammV2,
}

impl Dex {
    pub fn from_program(program: &Pubkey) -> Option<Dex> {
        [Dex::PumpAmm, Dex::MeteoraDlmm, Dex::MeteoraDammV2]
            .into_iter()
            .find(|dex| dex.program() == *program)
    }

    pub fn program(self) -> Pubkey {
        match self {
            Dex::PumpAmm => dex::pump_amm::ID,
            Dex::MeteoraDlmm => dex::meteora_dlmm::ID,
            Dex::MeteoraDammV2 => dex::meteora_damm_v2::ID,
        }
    }

    pub fn is_meteora(self) -> bool {
        matches!(self, Dex::MeteoraDlmm | Dex::MeteoraDammV2)
    }

    /// Position of the pool in the account list of this program's swap instructions.
    pub fn pool_account_index(self) -> usize {
        match self {
            Dex::PumpAmm | Dex::MeteoraDlmm => 0,
            // pool_authority comes first
            Dex::MeteoraDammV2 => 1,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Dex::PumpAmm => "pump_amm",
            Dex::MeteoraDlmm => "meteora_dlmm",
            Dex::MeteoraDammV2 => "meteora_damm_v2",
        }
    }
}

/// A pool account decoded just enough to follow it.
#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub dex: Dex,
    pub mints: [Pubkey; 2],
    /// Token accounts whose balance the bot reads for this pool (PumpSwap vaults).
    pub vaults: Vec<Pubkey>,
    /// Active bin of a DLMM pool.
    pub dlmm_active_id: Option<i32>,
}

impl PoolInfo {
    /// None when the account is not a pool of `dex`.
    pub fn parse(dex: Dex, account: &RawAccount) -> Option<PoolInfo> {
        if account.owner != dex.program() {
            return None;
        }
        let data = account.data.as_slice();
        match dex {
            Dex::PumpAmm => {
                if !data.starts_with(&dex::pump_amm::POOL_DISCRIMINATOR) {
                    return None;
                }
                let pool = dex::pump_amm::Pool::deserialize(data).ok()?;
                Some(PoolInfo {
                    dex,
                    mints: [pool.base_mint, pool.quote_mint],
                    vaults: vec![pool.pool_base_token_account, pool.pool_quote_token_account],
                    dlmm_active_id: None,
                })
            }
            Dex::MeteoraDlmm => {
                if !data.starts_with(&dex::meteora_dlmm::LB_PAIR_ACCOUNT_DISCM) {
                    return None;
                }
                let lb_pair = dex::meteora_dlmm::LbPairAccount::deserialize(data).ok()?.0;
                Some(PoolInfo {
                    dex,
                    mints: [lb_pair.token_x_mint, lb_pair.token_y_mint],
                    vaults: vec![],
                    dlmm_active_id: Some(lb_pair.active_id),
                })
            }
            Dex::MeteoraDammV2 => {
                if !data.starts_with(&dex::meteora_damm_v2::POOL_ACCOUNT_DISCM) {
                    return None;
                }
                let pool = dex::meteora_damm_v2::PoolAccount::deserialize(data).ok()?.0;
                Some(PoolInfo {
                    dex,
                    mints: [pool.token_a_mint, pool.token_b_mint],
                    vaults: vec![],
                    dlmm_active_id: None,
                })
            }
        }
    }

    /// The mints that identify the token(s) of this pool (quote mints left out).
    pub fn tokens(&self) -> impl Iterator<Item = &Pubkey> {
        self.mints.iter().filter(|mint| !QUOTE_MINTS.contains(mint))
    }
}

/// The DLMM pool a bin array belongs to, if `account` is a bin array.
pub fn dlmm_bin_array_lb_pair(account: &RawAccount) -> Option<Pubkey> {
    if account.owner != dex::meteora_dlmm::ID || !account.data.starts_with(&dex::meteora_dlmm::BIN_ARRAY_ACCOUNT_DISCM) {
        return None;
    }
    let bytes = account
        .data
        .get(BIN_ARRAY_LB_PAIR_OFFSET..BIN_ARRAY_LB_PAIR_OFFSET + 32)?;
    Pubkey::try_from(bytes).ok()
}

/// Bins per DLMM bin array.
const BINS_PER_ARRAY: i64 = 70;

/// Addresses of the bin arrays of `lb_pair` within `radius` arrays of the one holding `active_id`.
pub fn dlmm_bin_arrays_around(lb_pair: &Pubkey, active_id: i32, radius: i64) -> Vec<Pubkey> {
    let index = (active_id as i64).div_euclid(BINS_PER_ARRAY);
    (index - radius..=index + radius)
        .map(|index| dex::meteora_dlmm::pda::derive_bin_array_pda(lb_pair, index).0)
        .collect()
}

/// programSubscribe filters selecting the bin arrays of a DLMM pool.
pub fn dlmm_bin_array_filters(lb_pair: &Pubkey) -> Value {
    json!([
        {"memcmp": {"offset": 0, "bytes": bs58::encode(dex::meteora_dlmm::BIN_ARRAY_ACCOUNT_DISCM).into_string()}},
        {"memcmp": {"offset": BIN_ARRAY_LB_PAIR_OFFSET, "bytes": lb_pair.to_string()}},
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bin_arrays_around_the_active_bin() {
        let lb_pair = Pubkey::new_unique();
        let pda = |index: i64| dex::meteora_dlmm::pda::derive_bin_array_pda(&lb_pair, index).0;
        // bin -495 is in array -8 (bins -560..=-491), not -7
        assert_eq!(dlmm_bin_arrays_around(&lb_pair, -495, 1), vec![pda(-9), pda(-8), pda(-7)]);
        assert_eq!(dlmm_bin_arrays_around(&lb_pair, 69, 0), vec![pda(0)]);
        assert_eq!(dlmm_bin_arrays_around(&lb_pair, 70, 0), vec![pda(1)]);
        assert_eq!(dlmm_bin_arrays_around(&lb_pair, -1, 0), vec![pda(-1)]);
    }
}
