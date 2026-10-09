//! Follows the bot wallets and reports the PumpSwap / Meteora pools their successful transactions swap on.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info, warn};
use serde_json::Value;
use solana_program::pubkey::Pubkey;
use tokio::sync::mpsc::UnboundedSender;

use crate::pools::Dex;
use crate::rpc::{HttpRpc, SignatureInfo};

/// Max signatures per getSignaturesForAddress request.
const SIGNATURES_LIMIT: usize = 1000;
/// Swap instructions have at least this many accounts; smaller ones are event CPIs and the like.
const MIN_SWAP_ACCOUNTS: usize = 8;

pub static TRANSACTIONS_SCANNED: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct Candidate {
    pub pool: Pubkey,
    pub dex: Dex,
    pub wallet: Pubkey,
    pub block_time: i64,
}

pub async fn run(
    rpc: Arc<HttpRpc>,
    wallets: Vec<Pubkey>,
    bootstrap_tx_per_wallet: usize,
    poll_interval: Duration,
    candidates: UnboundedSender<Vec<Candidate>>,
) {
    // Newest signature already handled, per wallet. Missing until the wallet's first successful poll.
    let mut last_signature: HashMap<Pubkey, String> = HashMap::new();
    let mut bootstrapped = false;
    loop {
        for wallet in wallets.iter() {
            let until = last_signature.get(wallet).cloned();
            let signatures = match rpc.get_signatures(wallet, until.as_deref(), SIGNATURES_LIMIT).await {
                Ok(signatures) => signatures,
                Err(e) => {
                    warn!("Scanner: {wallet}: {e:#}");
                    continue;
                }
            };
            let Some(newest) = signatures.first() else { continue };
            last_signature.insert(*wallet, newest.signature.clone());
            // First read of a wallet: only its most recent transactions.
            let max_transactions = if until.is_none() { bootstrap_tx_per_wallet } else { usize::MAX };
            let successful: Vec<&SignatureInfo> = signatures.iter().filter(|s| s.success).take(max_transactions).collect();
            if until.is_none() {
                info!("Scanner: {wallet}: reading its last {} successful transactions", successful.len());
            }
            for signature in successful.into_iter().rev() {
                scan_transaction(&rpc, wallet, signature, &candidates).await;
            }
        }
        if !bootstrapped {
            bootstrapped = true;
            info!("Scanner: startup scan done, now following {} wallets", wallets.len());
        }
        if candidates.is_closed() {
            return;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn scan_transaction(
    rpc: &HttpRpc,
    wallet: &Pubkey,
    signature: &SignatureInfo,
    candidates: &UnboundedSender<Vec<Candidate>>,
) {
    let transaction = match rpc.get_transaction(&signature.signature).await {
        Ok(transaction) if !transaction.is_null() => transaction,
        Ok(_) => {
            debug!("Scanner: transaction {} not available yet", signature.signature);
            return;
        }
        Err(e) => {
            warn!("Scanner: {}: {e:#}", signature.signature);
            return;
        }
    };
    TRANSACTIONS_SCANNED.fetch_add(1, Ordering::Relaxed);
    let block_time = transaction["blockTime"].as_i64().unwrap_or(signature.block_time);
    let found: Vec<Candidate> = extract_pools(&transaction)
        .into_iter()
        .map(|(pool, dex)| Candidate {
            pool,
            dex,
            wallet: *wallet,
            block_time,
        })
        .collect();
    if !found.is_empty() {
        let _ = candidates.send(found);
    }
}

/// Pools swapped on in a `jsonParsed` transaction. Inner instructions are included because the
/// bots call the DEXes through their own program.
pub fn extract_pools(transaction: &Value) -> Vec<(Pubkey, Dex)> {
    let outer = transaction["transaction"]["message"]["instructions"]
        .as_array()
        .into_iter()
        .flatten();
    let inner = transaction["meta"]["innerInstructions"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|group| group["instructions"].as_array().into_iter().flatten());
    let mut pools = Vec::new();
    for instruction in outer.chain(inner) {
        let Some(dex) = instruction["programId"]
            .as_str()
            .and_then(|program| Pubkey::from_str(program).ok())
            .and_then(|program| Dex::from_program(&program))
        else {
            continue;
        };
        let Some(accounts) = instruction["accounts"].as_array() else { continue };
        if accounts.len() < MIN_SWAP_ACCOUNTS {
            continue;
        }
        let pool = accounts
            .get(dex.pool_account_index())
            .and_then(Value::as_str)
            .and_then(|pool| Pubkey::from_str(pool).ok());
        if let Some(pool) = pool {
            if !pools.contains(&(pool, dex)) {
                pools.push((pool, dex));
            }
        }
    }
    pools
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn accounts(first: &Pubkey, second: &Pubkey, len: usize) -> Vec<String> {
        let mut accounts = vec![first.to_string(), second.to_string()];
        accounts.resize(len, Pubkey::new_unique().to_string());
        accounts
    }

    #[test]
    fn finds_pools_in_outer_and_inner_instructions() {
        let pump_pool = Pubkey::new_unique();
        let damm_pool = Pubkey::new_unique();
        let dlmm_pool = Pubkey::new_unique();
        let authority = Pubkey::new_unique();
        let bot_program = Pubkey::new_unique();
        let transaction = json!({
            "transaction": {"message": {"instructions": [
                {"programId": bot_program.to_string(), "accounts": accounts(&bot_program, &authority, 20)},
                {"programId": dex::pump_amm::ID.to_string(), "accounts": accounts(&pump_pool, &authority, 17)},
            ]}},
            "meta": {"innerInstructions": [{"index": 0, "instructions": [
                {"programId": dex::meteora_damm_v2::ID.to_string(), "accounts": accounts(&authority, &damm_pool, 14)},
                {"programId": dex::meteora_dlmm::ID.to_string(), "accounts": accounts(&dlmm_pool, &authority, 15)},
                // event CPI: too few accounts
                {"programId": dex::meteora_dlmm::ID.to_string(), "accounts": [authority.to_string()]},
                // same pool twice
                {"programId": dex::pump_amm::ID.to_string(), "accounts": accounts(&pump_pool, &authority, 17)},
                // parsed instruction: no account list
                {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", "parsed": {"type": "transfer"}},
            ]}]},
        });
        assert_eq!(
            extract_pools(&transaction),
            vec![
                (pump_pool, Dex::PumpAmm),
                (damm_pool, Dex::MeteoraDammV2),
                (dlmm_pool, Dex::MeteoraDlmm),
            ]
        );
    }
}
