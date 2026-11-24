/// Base58 of a transaction signature.
///
/// 64-byte signatures go through `five8::encode_64`, the fixed-size encoder Solana's own
/// `Signature` display uses (byte-identical to `bs58`, tested below); `bs58`'s generic
/// encoder is O(n^2) and was ~13% of a monitor's busy CPU, called once per transaction per
/// pipeline. Other lengths keep `bs58`.
pub fn bytes_to_signature_str(bytes: &[u8]) -> String {
    let Ok(sig) = <&[u8; 64]>::try_from(bytes) else {
        return bs58::encode(bytes).into_string();
    };
    let mut out = [0u8; five8::BASE58_ENCODED_64_MAX_LEN];
    let len = five8::encode_64(sig, &mut out) as usize;
    // Base58 output is ASCII.
    String::from_utf8_lossy(&out[..len]).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_string_as_bs58() {
        let mut cases: Vec<Vec<u8>> =
            vec![vec![0; 64], vec![255; 64], vec![1; 64], vec![], vec![9; 32]];
        let mut lead = vec![0u8; 64];
        lead[63] = 7;
        cases.push(lead);
        // xorshift: deterministic pseudo-random signatures, some with leading zero bytes
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        for _ in 0..2000 {
            let mut s = vec![0u8; 64];
            for b in &mut s {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                *b = x as u8;
            }
            let zeros = (x % 5) as usize;
            s[..zeros].fill(0);
            cases.push(s);
        }
        for c in &cases {
            assert_eq!(
                bytes_to_signature_str(c),
                bs58::encode(c).into_string(),
                "{c:?}"
            );
        }
    }
}
