"""
SAP Skills Portal — Encrypted Credential Vault

Stores credentials in an AES-encrypted JSON file (Fernet).
The master password derives the encryption key via PBKDF2-HMAC-SHA256.

Vault file: /usr/sap/sap_skills/.vault.enc
Salt file:  /usr/sap/sap_skills/.vault.salt
"""

import json
import os
import base64
import hashlib
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes

VAULT_DIR = "/usr/sap/sap_skills"
VAULT_FILE = os.path.join(VAULT_DIR, ".vault.enc")
SALT_FILE = os.path.join(VAULT_DIR, ".vault.salt")


def _get_salt():
    """Get or create the salt for key derivation."""
    if os.path.exists(SALT_FILE):
        with open(SALT_FILE, "rb") as f:
            return f.read()
    salt = os.urandom(16)
    with open(SALT_FILE, "wb") as f:
        f.write(salt)
    os.chmod(SALT_FILE, 0o600)
    return salt


def _derive_key(master_password):
    """Derive a Fernet key from the master password using PBKDF2."""
    salt = _get_salt()
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=salt,
        iterations=480000,
    )
    key = base64.urlsafe_b64encode(kdf.derive(master_password.encode()))
    return Fernet(key)


def load_vault(master_password):
    """Decrypt and load the vault. Returns dict or None if wrong password."""
    if not os.path.exists(VAULT_FILE):
        return {}
    try:
        fernet = _derive_key(master_password)
        with open(VAULT_FILE, "rb") as f:
            encrypted = f.read()
        decrypted = fernet.decrypt(encrypted)
        return json.loads(decrypted)
    except InvalidToken:
        return None


def save_vault(master_password, data):
    """Encrypt and save the vault."""
    fernet = _derive_key(master_password)
    encrypted = fernet.encrypt(json.dumps(data, indent=2).encode())
    with open(VAULT_FILE, "wb") as f:
        f.write(encrypted)
    os.chmod(VAULT_FILE, 0o600)


def get_entry(master_password, system):
    """Get credentials for a system."""
    vault = load_vault(master_password)
    if vault is None:
        return None  # wrong password
    return vault.get(system)


def set_entry(master_password, system, credentials):
    """Set credentials for a system. credentials is a dict with user, password, etc."""
    vault = load_vault(master_password)
    if vault is None:
        return False  # wrong password
    vault[system] = credentials
    save_vault(master_password, vault)
    return True


def delete_entry(master_password, system):
    """Delete credentials for a system."""
    vault = load_vault(master_password)
    if vault is None:
        return False
    vault.pop(system, None)
    save_vault(master_password, vault)
    return True


def list_systems(master_password):
    """List all systems in the vault."""
    vault = load_vault(master_password)
    if vault is None:
        return None
    return list(vault.keys())


def init_vault(master_password):
    """Initialize a new empty vault with the given master password."""
    save_vault(master_password, {})
    return True
