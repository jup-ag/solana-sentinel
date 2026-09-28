use solana_pubkey::{pubkey, Pubkey};

pub const SPL_TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
pub const SPL_TOKEN_2022_PROGRAM_ID: Pubkey =
    pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
pub const SPL_ASSOCIATED_TOKEN_PROGRAM_ID: Pubkey =
    pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

pub const SQUADS_V4_PROGRAM_ID: Pubkey = pubkey!("SQDS4ep65T869zMMBKyuUq6aD6EgTu8psMjkvj52pCf");
pub const SQUADS_V3_PROGRAM_ID: Pubkey = pubkey!("SMPLKTQhrgo22hFCVq2VGX1KAktTWjeizkhrdB1eauK");
pub const SQUADS_MPL_PROGRAM_ID: Pubkey = pubkey!("SMPLecH534NA9acpos4G6x7uf3LWbCAwZQE9e8ZekMu");
pub const SQUADS_MPL_V2_PROGRAM_ID: Pubkey = pubkey!("SMPLKTQhrgo22hFCVq2VGX1KAktTWjeizkhrdB1eauK");
pub const SQUADS_TX_META_PROGRAM_ID: Pubkey =
    pubkey!("SMPL5bz5ERMdweouWrXtk3jmb6FnjZkWf7pHDsE6Zwz");
pub const COMPUTE_BUDGET_PROGRAM_ID: Pubkey =
    pubkey!("ComputeBudget111111111111111111111111111111");

pub const ASSOCIATED_TOKEN_PROGRAM_ID: Pubkey =
    pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

pub const USDC_MINT: Pubkey =
    Pubkey::from_str_const("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
pub const USDC_DECIMALS: u8 = 6;
pub const USDTB_MINT: Pubkey =
    Pubkey::from_str_const("8yXrtJ54jZtE84xEBzTESKuegjcAkAuDrdAhRd8i8n3T");
pub const USDTB_DECIMALS: u8 = 6;

pub const JUP_USD_MINT: Pubkey =
    Pubkey::from_str_const("JuprjznTrTSp2UFa3ZBUFgwdAmtZCq4MQCwysN55USD");
pub const JUP_USD_DECIMALS: u8 = 6;

pub const BPF_LOADER_PROGRAM_ID: Pubkey = pubkey!("BPFLoader1111111111111111111111111111111111");
pub const BPF_LOADER_2_PROGRAM_ID: Pubkey = pubkey!("BPFLoader2111111111111111111111111111111111");
pub const BPF_LOADER_UPGRADEABLE_PROGRAM_ID: Pubkey =
    pubkey!("BPFLoaderUpgradeab1e11111111111111111111111");

pub const MEMO_PROGRAM_ID: Pubkey = pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");
pub const MEMO_PROGRAM_V1_ID: Pubkey = pubkey!("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");

pub const MPL_TOKEN_METADATA_PROGRAM_ID: Pubkey =
    pubkey!("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");
pub const MPL_CORE_PROGRAM_ID: Pubkey = pubkey!("CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d");

pub const ED25519_PROGRAM_ID: Pubkey = pubkey!("Ed25519SigVerify111111111111111111111111111");

pub const STAKE_PROGRAM_ID: Pubkey = pubkey!("Stake11111111111111111111111111111111111111");

pub const ADDRESS_LOOKUP_TABLE_PROGRAM_ID: Pubkey =
    pubkey!("AddressLookupTab1e1111111111111111111111111");

pub const VOTE_STAKE_HISTORY_PROGRAM_ID: Pubkey =
    pubkey!("VoteMBhDCqGLRgYpp9o7DGyq81KNmwjXQRAHStjtJsS");

pub fn get_decimals_for_mint(mint: &Pubkey) -> Option<u8> {
    if *mint == USDC_MINT {
        Some(USDC_DECIMALS)
    } else if *mint == USDTB_MINT {
        Some(USDTB_DECIMALS)
    } else if *mint == JUP_USD_MINT {
        Some(JUP_USD_DECIMALS)
    } else {
        None
    }
}
