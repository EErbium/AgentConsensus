import hashlib
import json
import binascii
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.exceptions import InvalidSignature
from node_app.schemas.transaction import TransactionEnvelope


def compute_tx_id(
    proposer_node: str,
    sequence_number: int,
    timestamp: int,
    execution_payload_json: str,
) -> str:
    raw = (
        proposer_node
        + str(sequence_number)
        + str(timestamp)
        + execution_payload_json
    )
    return hashlib.sha256(raw.encode()).hexdigest()


def verify_signature(
    public_key_hex: str,
    message: bytes,
    signature_hex: str,
) -> bool:
    try:
        pub_key = Ed25519PublicKey.from_public_bytes(
            binascii.unhexlify(public_key_hex)
        )
        sig = binascii.unhexlify(signature_hex)
        pub_key.verify(sig, message)
        return True
    except (InvalidSignature, ValueError, binascii.Error):
        return False


def verify_envelope_signatures(
    envelope: TransactionEnvelope,
    public_keys: dict[str, str],
) -> list[str]:
    failed: list[str] = []
    msg = envelope.tx_id.encode()
    for signer_alias, sig_hex in envelope.signatures.items():
        pub_hex = public_keys.get(signer_alias)
        if pub_hex is None:
            failed.append(signer_alias)
        elif not verify_signature(pub_hex, msg, sig_hex):
            failed.append(signer_alias)
    return failed


def sign_payload(private_key_hex: str, message: bytes) -> str:
    sk = Ed25519PrivateKey.from_private_bytes(
        binascii.unhexlify(private_key_hex)
    )
    return binascii.hexlify(sk.sign(message)).decode()
