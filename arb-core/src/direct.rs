//! Arbitrage transactions made only of the DEX swap instructions, without the router and
//! flashloan programs: the wallet's own WSOL is swapped leg by leg, and the last swap requires
//! `amount + min_profit` out, so the whole transaction fails unless it ends with a profit.
//! With `wrap_sol`, the WSOL account is opened and funded from SOL at the start and closed at the
//! end, so the wallet only needs SOL.

use ahash::{AHashSet, HashMap};
use anyhow::{bail, ensure, Context};
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use spl_associated_token_account::instruction::create_associated_token_account_idempotent;
use utils::constants::WSOL;
use utils::types::MintPair;

use crate::arbitrage::{get_ixs, get_quote_result_for_ix_v3, lookup_ata_v2, QuoteResultForIx};
use crate::calculator::CalculatorEnum;
use crate::instruction::IxBuilder;

/// Anchor discriminator of DLMM / DAMM v2 `swap2`.
const SWAP2_IX_DISCM: [u8; 8] = [65, 75, 63, 76, 235, 91, 91, 136];
/// Offset of the minimum output in the exact-in swap instructions handled by `set_minimum_amount_out`.
const MINIMUM_AMOUNT_OUT_OFFSET: usize = 16;
/// Compute units of an idempotent associated token account creation.
const CREATE_ATA_COMPUTE_UNITS: u32 = 30_000;
/// Positions in a DLMM `swap` / `swap2` account list.
const DLMM_BITMAP_EXTENSION_INDEX: usize = 1;
const DLMM_TOKEN_X_MINT_INDEX: usize = 6;
const DLMM_TOKEN_Y_MINT_INDEX: usize = 7;
const DLMM_TOKEN_X_PROGRAM_INDEX: usize = 11;
const DLMM_TOKEN_Y_PROGRAM_INDEX: usize = 12;
/// `swap2` takes the memo program after the token programs.
const DLMM_MEMO_INDEX: usize = 13;
/// Compute units of the SOL transfer, sync_native and close_account around the swaps.
const WRAP_SOL_COMPUTE_UNITS: u32 = 15_000;

#[derive(Debug, Clone)]
pub struct DirectRoute {
    pub instructions: Vec<Instruction>,
    pub amount_in: u64,
    /// Output of the last swap according to the calculators.
    pub expected_out: u64,
    /// Minimum output enforced on the last swap.
    pub minimum_out: u64,
    pub compute_units: u32,
}

impl DirectRoute {
    pub fn expected_profit(&self) -> i64 {
        self.expected_out as i64 - self.amount_in as i64
    }
}

/// Builds the swaps of `calculators` for `amount` of `starting_mint`, quoted leg by leg.
#[allow(clippy::too_many_arguments)]
pub fn build_direct_route(
    slot: u64,
    amount: u64,
    calculators: &[Box<CalculatorEnum>],
    mint_pair_route: &[&MintPair],
    starting_mint: &Pubkey,
    owner: Pubkey,
    mint_to_ata: &mut HashMap<Pubkey, Pubkey>,
    allowed_token2022: &AHashSet<Pubkey>,
    min_profit: u64,
    wrap_sol: bool,
    leg_slack_bps: u64,
) -> anyhow::Result<DirectRoute> {
    ensure!(calculators.len() >= 2, "NotEnoughPools");
    ensure!(!wrap_sol || *starting_mint == WSOL, "WrapSolNeedsWsolStart");
    ensure!(calculators.len() == mint_pair_route.len(), "RouteLengthMismatch");
    let mut builder = IxBuilder::new(owner);
    let mut instructions = Vec::new();
    let mut compute_units = 0u32;
    let mut mint_in = *starting_mint;
    let mut amount_in = amount;
    let mut created_atas: AHashSet<Pubkey> = AHashSet::new();

    for (calculator, mint_pair) in calculators.iter().zip(mint_pair_route.iter()) {
        let mint_out = if mint_pair[0] == mint_in {
            mint_pair[1]
        } else if mint_pair[1] == mint_in {
            mint_pair[0]
        } else {
            bail!("RouteBroken: {mint_in} is not in {mint_pair:?}");
        };
        let QuoteResultForIx {
            amount_out,
            remaining_accounts,
            compute_units: leg_compute_units,
            ..
        } = get_quote_result_for_ix_v3(&mint_in, amount_in, slot, calculator, mint_pair)?;
        ensure!(amount_out > 0, "NoOutput");

        let source_ata = lookup_ata_v2(mint_to_ata, &owner, &mint_in, allowed_token2022);
        let destination_ata = lookup_ata_v2(mint_to_ata, &owner, &mint_out, allowed_token2022);
        if mint_out != *starting_mint && created_atas.insert(mint_out) {
            instructions.push(builder.get_create_ata_if_not_exists_v2(&mint_out, allowed_token2022));
            compute_units += CREATE_ATA_COMPUTE_UNITS;
        }
        let mut swap = get_ixs(
            calculator,
            amount_in,
            amount_out,
            &mint_in,
            mint_pair[0] == mint_in,
            source_ata,
            destination_ata,
            owner,
            remaining_accounts,
            allowed_token2022,
        )?;
        if swap.program_id == dex::meteora_dlmm::ID {
            upgrade_dlmm_swap(&mut swap, allowed_token2022)?;
        }
        instructions.push(swap);
        compute_units += leg_compute_units;
        mint_in = mint_out;
        amount_in = amount_out;
        // The next leg sells a bit less than quoted, so a slightly smaller output does not make it
        // fail for lack of tokens (the rest stays in the token account).
        if mint_in != *starting_mint {
            amount_in = (amount_in as u128 * (10_000 - leg_slack_bps.min(10_000)) as u128 / 10_000) as u64;
        }
    }
    ensure!(mint_in == *starting_mint, "RouteDoesNotLoop");

    let minimum_out = amount.checked_add(min_profit).context("MinimumOutOverflow")?;
    let last_swap = instructions.last_mut().context("NoSwap")?;
    set_minimum_amount_out(last_swap, minimum_out)?;

    if wrap_sol {
        let wsol_ata = lookup_ata_v2(mint_to_ata, &owner, &WSOL, allowed_token2022);
        let mut wrapped = vec![
            create_associated_token_account_idempotent(&owner, &owner, &WSOL, &spl_token::ID),
            solana_sdk::system_instruction::transfer(&owner, &wsol_ata, amount),
            spl_token::instruction::sync_native(&spl_token::ID, &wsol_ata)?,
        ];
        wrapped.append(&mut instructions);
        wrapped.push(spl_token::instruction::close_account(
            &spl_token::ID,
            &wsol_ata,
            &owner,
            &owner,
            &[],
        )?);
        instructions = wrapped;
        compute_units += CREATE_ATA_COMPUTE_UNITS + WRAP_SOL_COMPUTE_UNITS;
    }

    Ok(DirectRoute {
        instructions,
        amount_in: amount,
        expected_out: amount_in,
        minimum_out,
        compute_units,
    })
}

/// Turns a DLMM `swap` built by the bot into a `swap2` like the ones that land today: `swap2` handles
/// Token-2022 mints (memo program account, empty remaining accounts info), each mint gets the token
/// program it really uses, and the bin array bitmap extension, when present, is writable.
pub fn upgrade_dlmm_swap(instruction: &mut Instruction, token2022_mints: &AHashSet<Pubkey>) -> anyhow::Result<()> {
    ensure!(
        instruction.data.starts_with(&dex::meteora_dlmm::instructions::SWAP_IX_DISCM) && instruction.data.len() >= 24,
        "NotADlmmSwap"
    );
    ensure!(instruction.accounts.len() > DLMM_MEMO_INDEX, "DlmmSwapTooFewAccounts");
    let mut data = SWAP2_IX_DISCM.to_vec();
    data.extend_from_slice(&instruction.data[8..24]);
    // remaining_accounts_info: empty Vec<RemainingAccountsSlice>
    data.extend_from_slice(&0u32.to_le_bytes());
    instruction.data = data;

    let token_program = |mint: &Pubkey| {
        if token2022_mints.contains(mint) {
            spl_token_2022::ID
        } else {
            spl_token::ID
        }
    };
    let token_x_program = token_program(&instruction.accounts[DLMM_TOKEN_X_MINT_INDEX].pubkey);
    let token_y_program = token_program(&instruction.accounts[DLMM_TOKEN_Y_MINT_INDEX].pubkey);
    instruction.accounts[DLMM_TOKEN_X_PROGRAM_INDEX].pubkey = token_x_program;
    instruction.accounts[DLMM_TOKEN_Y_PROGRAM_INDEX].pubkey = token_y_program;
    let bitmap_extension = &mut instruction.accounts[DLMM_BITMAP_EXTENSION_INDEX];
    bitmap_extension.is_writable = bitmap_extension.pubkey != dex::meteora_dlmm::ID;
    instruction
        .accounts
        .insert(DLMM_MEMO_INDEX, solana_sdk::instruction::AccountMeta::new_readonly(spl_memo::ID, false));
    Ok(())
}

/// Sets the minimum output of an exact-in swap: PumpSwap `sell`, DLMM / DAMM v2 `swap` and `swap2`,
/// whose data is `discriminator | amount_in u64 | minimum_amount_out u64 | ...`.
pub fn set_minimum_amount_out(instruction: &mut Instruction, minimum_amount_out: u64) -> anyhow::Result<()> {
    let exact_in = if instruction.program_id == dex::pump_amm::ID {
        instruction.data.starts_with(&dex::pump_amm::SELL_IX_DISCM)
    } else if instruction.program_id == dex::meteora_dlmm::ID || instruction.program_id == dex::meteora_damm_v2::ID {
        instruction.data.starts_with(&dex::meteora_dlmm::instructions::SWAP_IX_DISCM) || instruction.data.starts_with(&SWAP2_IX_DISCM)
    } else {
        false
    };
    ensure!(
        exact_in && instruction.data.len() >= MINIMUM_AMOUNT_OUT_OFFSET + 8,
        "UnsupportedLastSwap: {}",
        instruction.program_id
    );
    instruction.data[MINIMUM_AMOUNT_OUT_OFFSET..MINIMUM_AMOUNT_OUT_OFFSET + 8]
        .copy_from_slice(&minimum_amount_out.to_le_bytes());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn instruction(program_id: Pubkey, discriminator: [u8; 8]) -> Instruction {
        let mut data = discriminator.to_vec();
        data.extend(1_000u64.to_le_bytes());
        data.extend(5u64.to_le_bytes());
        Instruction {
            program_id,
            accounts: vec![],
            data,
        }
    }

    #[test]
    fn sets_minimum_out_of_exact_in_swaps() {
        for (program, discriminator) in [
            (dex::pump_amm::ID, dex::pump_amm::SELL_IX_DISCM),
            (dex::meteora_dlmm::ID, dex::meteora_dlmm::instructions::SWAP_IX_DISCM),
            (dex::meteora_dlmm::ID, SWAP2_IX_DISCM),
            (dex::meteora_damm_v2::ID, dex::meteora_damm_v2::SWAP_IX_DISCM),
        ] {
            let mut ix = instruction(program, discriminator);
            set_minimum_amount_out(&mut ix, 123_456).unwrap();
            assert_eq!(&ix.data[8..16], &1_000u64.to_le_bytes(), "amount in kept");
            assert_eq!(&ix.data[16..24], &123_456u64.to_le_bytes());
        }
    }

    #[test]
    fn dlmm_swap_becomes_swap2() {
        let token2022_mint = Pubkey::new_unique();
        let extension = Pubkey::new_unique();
        let mut accounts: Vec<solana_sdk::instruction::AccountMeta> = (0..16)
            .map(|_| solana_sdk::instruction::AccountMeta::new_readonly(Pubkey::new_unique(), false))
            .collect();
        accounts[DLMM_BITMAP_EXTENSION_INDEX].pubkey = extension;
        accounts[DLMM_TOKEN_X_MINT_INDEX].pubkey = token2022_mint;
        accounts[DLMM_TOKEN_Y_MINT_INDEX].pubkey = WSOL;
        let event_authority = accounts[13].pubkey;
        let mut ix = instruction(dex::meteora_dlmm::ID, dex::meteora_dlmm::instructions::SWAP_IX_DISCM);
        ix.accounts = accounts;

        upgrade_dlmm_swap(&mut ix, &AHashSet::from_iter([token2022_mint])).unwrap();

        assert_eq!(&ix.data[..8], &SWAP2_IX_DISCM);
        assert_eq!(&ix.data[8..24], &[1_000u64.to_le_bytes(), 5u64.to_le_bytes()].concat()[..]);
        assert_eq!(&ix.data[24..], &0u32.to_le_bytes(), "empty remaining accounts info");
        assert_eq!(ix.accounts.len(), 17);
        assert_eq!(ix.accounts[DLMM_TOKEN_X_PROGRAM_INDEX].pubkey, spl_token_2022::ID);
        assert_eq!(ix.accounts[DLMM_TOKEN_Y_PROGRAM_INDEX].pubkey, spl_token::ID);
        assert_eq!(ix.accounts[DLMM_MEMO_INDEX].pubkey, spl_memo::ID);
        assert_eq!(ix.accounts[DLMM_MEMO_INDEX + 1].pubkey, event_authority);
        assert!(ix.accounts[DLMM_BITMAP_EXTENSION_INDEX].is_writable);
    }

    #[test]
    fn refuses_exact_out_swaps() {
        // A PumpSwap buy fixes its output; its second argument is a maximum input.
        let mut ix = instruction(dex::pump_amm::ID, dex::pump_amm::BUY_IX_DISCM);
        assert!(set_minimum_amount_out(&mut ix, 1).is_err());
    }
}
