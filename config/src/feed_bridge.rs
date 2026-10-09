use serde::{Deserialize, Serialize};

/// Live feed without a Geyser plugin (`feed-bridge` binary): follows arbitrage bot wallets to pick
/// the pools to watch, streams them over a WebSocket RPC and republishes every account update on
/// the ZeroMQ sockets arb-bot reads.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct FeedBridgeConfig {
    /// HTTP RPC used to follow the wallets and to fetch accounts
    /// (getSignaturesForAddress, getTransaction, getMultipleAccounts).
    pub rpc: String,
    /// Requests per second allowed on `rpc`.
    pub rpc_rps: u32,
    /// Meteora DLMM bin arrays followed on each side of the array holding the active bin.
    pub bin_array_radius: i64,
    /// Where account updates come from: "ws" (WebSocket RPC) or "grpc" (Yellowstone gRPC).
    pub source: String,
    /// WebSocket RPC used to stream the account updates (source = "ws").
    pub ws: String,
    /// Yellowstone gRPC endpoint (source = "grpc"), e.g. https://grpc.ny.shyft.to
    pub grpc_url: String,
    /// x-token of the gRPC endpoint.
    pub grpc_token: String,
    /// With gRPC, the bin arrays around the active bin of the followed DLMM pools are fetched
    /// every N seconds to follow those the price moved to (0 = never).
    pub bin_array_refresh_secs: u64,
    /// Max subscriptions per WebSocket connection (Helius refuses more than 1000).
    pub max_subscriptions_per_connection: usize,

    /// Fixed list of pools to follow instead of the pools traded by the bot wallets (e.g. on
    /// devnet). The selection rule (`require_pump_and_meteora`) still applies.
    pub static_pools: Vec<String>,
    /// File with one bot wallet address per line (`#` starts a comment).
    pub wallets_file: String,
    /// Successful transactions read per wallet at startup.
    pub bootstrap_tx_per_wallet: usize,
    /// Delay between two polls of the wallets, in milliseconds.
    pub poll_interval_ms: u64,
    /// Only follow tokens seen with both a PumpSwap and a Meteora pool.
    pub require_pump_and_meteora: bool,
    /// The followed tokens and pools are written to this JSON file.
    pub state_file: String,

    /// All followed accounts are republished every N seconds (0 = only when a subscriber connects).
    pub keyframe_interval_secs: u64,
    pub accounts_socket: String,
    pub slot_socket: String,
}

impl Default for FeedBridgeConfig {
    fn default() -> Self {
        FeedBridgeConfig {
            rpc: String::new(),
            rpc_rps: 8,
            bin_array_radius: 5,
            source: "ws".to_string(),
            ws: String::new(),
            grpc_url: String::new(),
            grpc_token: String::new(),
            bin_array_refresh_secs: 300,
            max_subscriptions_per_connection: 900,
            static_pools: Vec::new(),
            wallets_file: "bot_wallets.txt".to_string(),
            bootstrap_tx_per_wallet: 150,
            poll_interval_ms: 2000,
            require_pump_and_meteora: true,
            state_file: "bot_selection.json".to_string(),
            keyframe_interval_secs: 60,
            accounts_socket: "ipc:///tmp/accounts_zmq_v2.sock".to_string(),
            slot_socket: "ipc:///tmp/slot_zmq_v2.sock".to_string(),
        }
    }
}
