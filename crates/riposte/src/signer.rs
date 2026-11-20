use crate::traits::TxSigner;
use anyhow::{anyhow, bail, ensure, Context};
use async_trait::async_trait;
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::{KeySpec, MessageType, SigningAlgorithmSpec};
use domain::alert::Alert;
use solana_keypair::{read_keypair_file, Keypair};
use solana_message::VersionedMessage;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use solana_signer::Signer;
use std::path::Path;

/// A local keypair. The key lives in this process, so devnet and tests only.
pub struct KeypairSigner(Keypair);

impl KeypairSigner {
    pub fn new(keypair: Keypair) -> Self {
        Self(keypair)
    }

    pub fn from_file(path: &Path) -> anyhow::Result<Self> {
        read_keypair_file(path)
            .map(Self)
            .map_err(|e| anyhow!("failed to read keypair `{}`: {e}", path.display()))
    }
}

#[async_trait]
impl TxSigner for KeypairSigner {
    fn pubkey(&self) -> Pubkey {
        self.0.pubkey()
    }

    async fn sign(&self, msg: &VersionedMessage, _trigger: &Alert) -> anyhow::Result<Signature> {
        Ok(self.0.sign_message(&msg.serialize()))
    }
}

/// DER SubjectPublicKeyInfo prefix of an Ed25519 key (RFC 8410); the raw key follows.
const ED25519_SPKI_PREFIX: [u8; 12] = [
    0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
];

/// An `ECC_NIST_EDWARDS25519` key in AWS KMS; the private key never leaves KMS.
pub struct KmsSigner {
    client: aws_sdk_kms::Client,
    key_id: String,
    pubkey: Pubkey,
}

impl KmsSigner {
    /// Credentials and region come from the default AWS chain unless `region` is set.
    pub async fn connect(key_id: &str, region: Option<&str>) -> anyhow::Result<Self> {
        let mut loader = aws_config::from_env();
        if let Some(region) = region {
            loader = loader.region(aws_config::Region::new(region.to_owned()));
        }
        let client = aws_sdk_kms::Client::new(&loader.load().await);
        let out = client
            .get_public_key()
            .key_id(key_id)
            .send()
            .await
            .with_context(|| format!("KMS GetPublicKey for `{key_id}` failed"))?;
        ensure!(
            out.key_spec() == Some(&KeySpec::EccNistEdwards25519),
            "KMS key `{key_id}` has spec {:?}, need ECC_NIST_EDWARDS25519",
            out.key_spec()
        );
        let der = out.public_key().context("KMS returned no public key")?;
        let pubkey = ed25519_pubkey_from_spki(der.as_ref())
            .with_context(|| format!("KMS key `{key_id}` public key"))?;
        Ok(Self {
            client,
            key_id: key_id.to_owned(),
            pubkey,
        })
    }
}

fn ed25519_pubkey_from_spki(der: &[u8]) -> anyhow::Result<Pubkey> {
    match der.split_at_checked(ED25519_SPKI_PREFIX.len()) {
        Some((prefix, key)) if prefix == ED25519_SPKI_PREFIX && key.len() == 32 => {
            Ok(Pubkey::new_from_array(key.try_into()?))
        }
        _ => bail!(
            "not a DER Ed25519 SubjectPublicKeyInfo ({} bytes)",
            der.len()
        ),
    }
}

#[async_trait]
impl TxSigner for KmsSigner {
    fn pubkey(&self) -> Pubkey {
        self.pubkey
    }

    async fn sign(&self, msg: &VersionedMessage, _trigger: &Alert) -> anyhow::Result<Signature> {
        let bytes = msg.serialize();
        let out = self
            .client
            .sign()
            .key_id(&self.key_id)
            .message(Blob::new(bytes.clone()))
            .message_type(MessageType::Raw)
            .signing_algorithm(SigningAlgorithmSpec::Ed25519Sha512)
            .send()
            .await
            .context("KMS Sign failed")?;
        let raw: [u8; 64] = out
            .signature()
            .context("KMS returned no signature")?
            .as_ref()
            .try_into()
            .context("KMS signature is not 64 bytes")?;
        let signature = Signature::from(raw);
        // A bad signature would only surface as a dropped tx; fail here instead.
        ensure!(
            signature.verify(self.pubkey.as_ref(), &bytes),
            "KMS signature does not verify against {}",
            self.pubkey
        );
        Ok(signature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_rfc8410_ed25519_spki() {
        // RFC 8410 §10.1 example public key, base64 MCowBQYDK2VwAyEAGb9ECWmEzf6FQbrBZ9w7lshQhqowtrbLDFw4rXAxZuE=
        let raw = [
            0x19, 0xbf, 0x44, 0x09, 0x69, 0x84, 0xcd, 0xfe, 0x85, 0x41, 0xba, 0xc1, 0x67, 0xdc,
            0x3b, 0x96, 0xc8, 0x50, 0x86, 0xaa, 0x30, 0xb6, 0xb6, 0xcb, 0x0c, 0x5c, 0x38, 0xad,
            0x70, 0x31, 0x66, 0xe1,
        ];
        let der = [&ED25519_SPKI_PREFIX[..], &raw].concat();
        assert_eq!(
            ed25519_pubkey_from_spki(&der).unwrap(),
            Pubkey::new_from_array(raw)
        );
        assert!(ed25519_pubkey_from_spki(&der[..43]).is_err());
    }
}
