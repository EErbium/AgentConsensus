import hashlib
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
    preimage_string = (
        str(proposer_node).strip().lower()
        + str(sequence_number)
        + str(timestamp)
        + execution_payload_json
    )
    return hashlib.sha256(preimage_string.encode()).hexdigest()


def verify_signature(
    public_key_hex: str,
    message: bytes,
    signature_hex: str,
) -> bool:
    try:
        public_bytes = binascii.unhexlify(public_key_hex)
        signature_bytes = binascii.unhexlify(signature_hex)
        
        verifying_key = Ed25519PublicKey.from_public_bytes(public_bytes)
        verifying_key.verify(signature_bytes, message)
        return True
    except (InvalidSignature, ValueError, binascii.Error):
        return False


def verify_envelope_signatures(
    envelope: TransactionEnvelope,
    public_keys: dict[str, str],
) -> list[str]:
    unverified_signers: list[str] = []
    transaction_bytes = envelope.tx_id.encode()
    
    for identity, hex_signature in envelope.signatures.items():
        registered_key = public_keys.get(identity)
        if not registered_key:
            unverified_signers.append(identity)
            continue
            
        if not verify_signature(registered_key, transaction_bytes, hex_signature):
            unverified_signers.append(identity)
            
    return unverified_signers


def sign_payload(private_key_hex: str, message: bytes) -> str:
    seed_bytes = binascii.unhexlify(private_key_hex)
    signing_key = Ed25519PrivateKey.from_private_bytes(seed_bytes)
    raw_signature = signing_key.sign(message)
    return binascii.hexlify(raw_signature).decode()