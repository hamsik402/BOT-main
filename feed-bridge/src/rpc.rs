//! JSON-RPC over HTTP with a request-per-second limit shared by every caller.

use std::str::FromStr;
use std::time::Duration;

use anyhow::{bail, Context};
use base64::Engine;
use serde_json::{json, Value};
use solana_program::pubkey::Pubkey;
use tokio::sync::Mutex;
use tokio::time::Instant;

const MAX_ATTEMPTS: u64 = 6;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// Max accounts per `getMultipleAccounts` request.
const MULTIPLE_ACCOUNTS_LIMIT: usize = 100;

#[derive(Debug, Clone)]
pub struct RawAccount {
    pub owner: Pubkey,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SignatureInfo {
    pub signature: String,
    pub success: bool,
    pub block_time: i64,
}

pub struct HttpRpc {
    name: &'static str,
    url: String,
    client: reqwest::Client,
    interval: Duration,
    next_request: Mutex<Instant>,
}

impl HttpRpc {
    pub fn new(name: &'static str, url: &str, requests_per_second: u32) -> Self {
        HttpRpc {
            name,
            url: url.to_string(),
            client: reqwest::Client::builder()
                .timeout(REQUEST_TIMEOUT)
                .build()
                .expect("Failed to build the HTTP client"),
            interval: Duration::from_secs_f64(1.0 / requests_per_second.max(1) as f64),
            next_request: Mutex::new(Instant::now()),
        }
    }

    /// Waits until this caller may send a request.
    async fn wait_turn(&self) {
        let mut next_request = self.next_request.lock().await;
        let now = Instant::now();
        if *next_request > now {
            tokio::time::sleep_until(*next_request).await;
        }
        *next_request = (*next_request).max(now) + self.interval;
    }

    /// Calls `method` and returns its `result`. Retries on HTTP 429, 5xx and network errors.
    pub async fn call(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).to_string();
        let mut last_error = String::new();
        for attempt in 1..=MAX_ATTEMPTS {
            self.wait_turn().await;
            let response = self
                .client
                .post(&self.url)
                .header("content-type", "application/json")
                .body(body.clone())
                .send()
                .await;
            match response {
                Ok(response) => {
                    let status = response.status();
                    let bytes = response.bytes().await.unwrap_or_default();
                    if status.as_u16() == 429 || status.is_server_error() {
                        last_error = format!("HTTP {status}");
                    } else {
                        let mut value: Value = serde_json::from_slice(&bytes).with_context(|| {
                            format!("{} {method}: HTTP {status}: {}", self.name, String::from_utf8_lossy(&bytes))
                        })?;
                        if let Some(error) = value.get("error") {
                            bail!("{} {method}: {error}", self.name);
                        }
                        return Ok(value["result"].take());
                    }
                }
                Err(e) => last_error = e.to_string(),
            }
            tokio::time::sleep(Duration::from_millis(250 * attempt)).await;
        }
        bail!("{} {method}: giving up after {MAX_ATTEMPTS} attempts ({last_error})", self.name)
    }

    /// Returns every key with its account (None when it does not exist) and the slot it was read at.
    pub async fn get_multiple_accounts(&self, keys: &[Pubkey]) -> anyhow::Result<Vec<(Pubkey, Option<RawAccount>, u64)>> {
        let mut accounts = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(MULTIPLE_ACCOUNTS_LIMIT) {
            let chunk_keys: Vec<String> = chunk.iter().map(|key| key.to_string()).collect();
            let result = self
                .call(
                    "getMultipleAccounts",
                    json!([chunk_keys, {"encoding": "base64", "commitment": "processed"}]),
                )
                .await?;
            let slot = result["context"]["slot"].as_u64().unwrap_or(0);
            let values = result["value"].as_array().context("getMultipleAccounts: no value")?;
            accounts.extend(chunk.iter().zip(values).map(|(key, value)| (*key, parse_account(value), slot)));
        }
        Ok(accounts)
    }

    /// Newest first. With `until`, only the signatures more recent than it.
    pub async fn get_signatures(
        &self,
        address: &Pubkey,
        until: Option<&str>,
        limit: usize,
    ) -> anyhow::Result<Vec<SignatureInfo>> {
        let mut options = json!({"limit": limit, "commitment": "confirmed"});
        if let Some(until) = until {
            options["until"] = json!(until);
        }
        let result = self
            .call("getSignaturesForAddress", json!([address.to_string(), options]))
            .await?;
        let values = result.as_array().context("getSignaturesForAddress: not an array")?;
        Ok(values
            .iter()
            .filter_map(|value| {
                Some(SignatureInfo {
                    signature: value["signature"].as_str()?.to_string(),
                    success: value["err"].is_null(),
                    block_time: value["blockTime"].as_i64().unwrap_or(0),
                })
            })
            .collect())
    }

    /// The transaction in `jsonParsed` encoding (Null when the RPC does not have it yet).
    pub async fn get_transaction(&self, signature: &str) -> anyhow::Result<Value> {
        self.call(
            "getTransaction",
            json!([signature, {
                "encoding": "jsonParsed",
                "commitment": "confirmed",
                "maxSupportedTransactionVersion": 1,
            }]),
        )
        .await
    }
}

/// Parses an RPC account (`{"owner": ..., "data": [<base64>, "base64"]}`); None for a missing account.
pub fn parse_account(value: &Value) -> Option<RawAccount> {
    let owner = Pubkey::from_str(value.get("owner")?.as_str()?).ok()?;
    let encoded = value.get("data")?.get(0)?.as_str()?;
    let data = base64::engine::general_purpose::STANDARD.decode(encoded).ok()?;
    Some(RawAccount { owner, data })
}
