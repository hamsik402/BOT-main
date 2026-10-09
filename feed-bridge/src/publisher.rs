//! ZeroMQ side: the sockets arb-bot connects to.

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use anyhow::Context;
use log::{info, warn};
use solana_program::pubkey::Pubkey;
use tokio::sync::mpsc::UnboundedSender;

/// Status sent with each slot on the slot socket.
const SLOT_STATUS: &[u8] = b"processed";
const POLL_INTERVAL: Duration = Duration::from_millis(20);
/// First byte of the frame an XPUB socket receives when a subscriber subscribes.
const SUBSCRIBE_FRAME: u8 = 1;

pub enum Outgoing {
    /// Payload built by `encode_accounts`.
    Accounts(Vec<u8>),
    Slot(u64),
}

/// Serializes accounts in the format of `utils::deserialize::deserialize_v2`, i.e. for each account
/// `data length (u32 LE) | slot (u64 LE) | pubkey | owner | data`.
pub fn encode_accounts<'a>(accounts: impl IntoIterator<Item = (&'a Pubkey, &'a Pubkey, &'a [u8], u64)>) -> Vec<u8> {
    let mut payload = Vec::new();
    for (pubkey, owner, data, slot) in accounts {
        payload.extend_from_slice(&(data.len() as u32).to_le_bytes());
        payload.extend_from_slice(&slot.to_le_bytes());
        payload.extend_from_slice(pubkey.as_ref());
        payload.extend_from_slice(owner.as_ref());
        payload.extend_from_slice(data);
    }
    payload
}

/// Binds the sockets and publishes from a dedicated thread. A unit is sent on `subscribers` each
/// time a subscriber connects to the accounts socket, so it can be sent a full snapshot.
pub fn spawn(
    accounts_endpoint: &str,
    slot_endpoint: &str,
    outgoing: Receiver<Outgoing>,
    subscribers: UnboundedSender<()>,
) -> anyhow::Result<()> {
    let context = zmq2::Context::new();
    let accounts = context.socket(zmq2::XPUB)?;
    accounts.set_xpub_verbose(true)?;
    accounts
        .bind(accounts_endpoint)
        .with_context(|| format!("Failed to bind {accounts_endpoint}"))?;
    let slots = context.socket(zmq2::PUB)?;
    slots
        .bind(slot_endpoint)
        .with_context(|| format!("Failed to bind {slot_endpoint}"))?;
    info!("ZeroMQ: publishing accounts on {accounts_endpoint}, slots on {slot_endpoint}");

    std::thread::Builder::new().name("zmq-publisher".to_string()).spawn(move || {
        let _context = context;
        loop {
            while let Ok(frame) = accounts.recv_bytes(zmq2::DONTWAIT) {
                if frame.first() == Some(&SUBSCRIBE_FRAME) {
                    info!("ZeroMQ: new subscriber");
                    let _ = subscribers.send(());
                }
            }
            match outgoing.recv_timeout(POLL_INTERVAL) {
                Ok(Outgoing::Accounts(payload)) => {
                    if let Err(e) = accounts.send(payload, 0) {
                        warn!("ZeroMQ: failed to publish accounts: {e}");
                    }
                }
                Ok(Outgoing::Slot(slot)) => {
                    let mut payload = slot.to_le_bytes().to_vec();
                    payload.extend_from_slice(SLOT_STATUS);
                    if let Err(e) = slots.send(payload, 0) {
                        warn!("ZeroMQ: failed to publish slot: {e}");
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return,
            }
        }
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_accounts_are_read_back_by_the_bot() {
        let accounts = [
            (Pubkey::new_unique(), Pubkey::new_unique(), vec![1u8, 2, 3], 10u64),
            (Pubkey::new_unique(), Pubkey::new_unique(), vec![], 11),
            (Pubkey::new_unique(), Pubkey::new_unique(), vec![7u8; 300], 12),
        ];
        let payload = encode_accounts(
            accounts
                .iter()
                .map(|(pubkey, owner, data, slot)| (pubkey, owner, data.as_slice(), *slot)),
        );
        let decoded = utils::deserialize::deserialize_v2(payload).unwrap();
        assert_eq!(decoded.slot, 10);
        assert_eq!(decoded.message.len(), accounts.len());
        for (message, (pubkey, owner, data, _)) in decoded.message.iter().zip(accounts.iter()) {
            assert_eq!(message.pubkey, *pubkey);
            assert_eq!(message.owner, *owner);
            assert_eq!(message.data, *data);
        }
    }
}
