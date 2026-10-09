//! `feed-bridge <config> listen`: prints what arrives on the ZeroMQ sockets, to check the bridge
//! without starting the bot.

use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

use config::feed_bridge::FeedBridgeConfig;
use solana_program::pubkey::Pubkey;
use utils::deserialize::deserialize_v2;

const REPORT_INTERVAL: Duration = Duration::from_secs(10);
const POLL_TIMEOUT_MS: i64 = 1000;

fn owner_name(owner: &Pubkey) -> String {
    if *owner == dex::pump_amm::ID {
        "pump_amm".to_string()
    } else if *owner == dex::meteora_dlmm::ID {
        "meteora_dlmm".to_string()
    } else if *owner == dex::meteora_damm_v2::ID {
        "meteora_damm_v2".to_string()
    } else if owner.to_string() == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" {
        "token".to_string()
    } else if owner.to_string() == "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" {
        "token_2022".to_string()
    } else {
        owner.to_string()
    }
}

pub fn run(cfg: &FeedBridgeConfig) -> anyhow::Result<()> {
    let context = zmq2::Context::new();
    let accounts = context.socket(zmq2::SUB)?;
    accounts.connect(&cfg.accounts_socket)?;
    accounts.set_subscribe(b"")?;
    let slots = context.socket(zmq2::SUB)?;
    slots.connect(&cfg.slot_socket)?;
    slots.set_subscribe(b"")?;
    println!("Listening on {} and {}", cfg.accounts_socket, cfg.slot_socket);

    let mut messages = 0u64;
    let mut updates_by_owner: BTreeMap<String, u64> = BTreeMap::new();
    let mut distinct: HashSet<Pubkey> = HashSet::new();
    let mut errors = 0u64;
    let mut slot_messages = 0u64;
    let mut last_slot = 0u64;
    let mut last_account_slot = 0u64;
    let mut next_report = Instant::now() + REPORT_INTERVAL;
    loop {
        let (accounts_ready, slots_ready) = {
            let mut items = [accounts.as_poll_item(zmq2::POLLIN), slots.as_poll_item(zmq2::POLLIN)];
            zmq2::poll(&mut items, POLL_TIMEOUT_MS)?;
            (items[0].is_readable(), items[1].is_readable())
        };
        if accounts_ready {
            let payload = accounts.recv_bytes(0)?;
            match deserialize_v2(payload) {
                Ok(decoded) => {
                    messages += 1;
                    last_account_slot = last_account_slot.max(decoded.slot);
                    for message in decoded.message {
                        *updates_by_owner.entry(owner_name(&message.owner)).or_default() += 1;
                        distinct.insert(message.pubkey);
                    }
                }
                Err(_) => errors += 1,
            }
        }
        if slots_ready {
            let payload = slots.recv_bytes(0)?;
            if let Some(bytes) = payload.get(..8) {
                last_slot = u64::from_le_bytes(bytes.try_into()?);
                slot_messages += 1;
            }
        }
        if Instant::now() >= next_report {
            println!(
                "{messages} messages, {} accounts ({errors} undecodable), by owner {updates_by_owner:?}; \
                 {slot_messages} slots, last slot {last_slot}, last account slot {last_account_slot}",
                distinct.len()
            );
            messages = 0;
            errors = 0;
            slot_messages = 0;
            updates_by_owner.clear();
            distinct.clear();
            next_report = Instant::now() + REPORT_INTERVAL;
        }
    }
}
