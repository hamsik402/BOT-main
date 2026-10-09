use serde::{Deserialize, Serialize};

/// `direct` provider: sends arbitrages made only of DEX swap instructions (no router / flashloan
/// program), paid with the wallet's own WSOL. Every transaction is simulated before it is sent.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct DirectConfig {
    /// WSOL put in one arbitrage, in lamports.
    pub amount: u64,
    /// The last swap must return `amount + min_profit` lamports or the transaction fails.
    pub min_profit: u64,
    /// Each leg after the first sells this much less than the previous leg is quoted to return,
    /// in basis points, so a slightly smaller output does not make the transaction fail.
    pub leg_slack_bps: u64,
    /// Expected profit (from the calculators) needed to try an opportunity, in lamports.
    pub min_expected_profit: u64,
    /// Priority fee, in micro-lamports per compute unit.
    pub compute_unit_price: u64,
    /// Added to the estimated compute units, in percent.
    pub compute_unit_margin_percent: u32,
    /// Wrap `amount` SOL into WSOL at the start of each transaction and unwrap at the end, so the
    /// wallet only needs SOL. Otherwise the wallet's WSOL account must hold `amount`.
    pub wrap_sol: bool,
    /// Address lookup tables used to fit the transactions in 1232 bytes (any existing table can
    /// be used, e.g. public ones holding the token / DEX program accounts).
    pub lookup_tables: Vec<String>,

    /// Simulate only, never send.
    pub simulate_only: bool,
    /// When set, transactions are built and simulated for this wallet instead of the bot's own
    /// (without signature check), to test the whole chain without funds. Implies `simulate_only`.
    pub simulate_as: String,
    /// RPC used for simulations and balances.
    pub rpc: String,
    pub rpc_requests_per_second: u32,
    /// RPC used to send transactions (e.g. a SWQoS endpoint).
    pub send_rpc: String,
    pub sends_per_second: u32,

    /// No more transactions are sent after this many.
    pub max_transactions: u64,
    /// Sending stops when the wallet holds less than this (SOL + WSOL), in lamports.
    pub stop_below_balance: u64,
}

impl Default for DirectConfig {
    fn default() -> Self {
        DirectConfig {
            amount: 50_000_000,
            min_profit: 10_000,
            leg_slack_bps: 20,
            min_expected_profit: 100_000,
            compute_unit_price: 1_000,
            compute_unit_margin_percent: 20,
            wrap_sol: true,
            lookup_tables: Vec::new(),
            simulate_only: true,
            simulate_as: String::new(),
            rpc: String::new(),
            rpc_requests_per_second: 20,
            send_rpc: String::new(),
            sends_per_second: 5,
            max_transactions: 200,
            stop_below_balance: 0,
        }
    }
}
