pub fn bytes_to_signature_str(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}
