import rsa
import base64

def encrypt_text(text: str, public_key: rsa.PublicKey):
    byte_text = text.encode()
    encrypted_text = rsa.encrypt(byte_text, public_key).hex()
    
    return encrypted_text

def decrypt_text(text: str, private_key: rsa.PrivateKey):
    encrypted_byte_text = bytes.fromhex(text)
    decrypted_text = rsa.decrypt(encrypted_byte_text, private_key).decode()
    
    return decrypted_text

def convert_base64_private_key_to_rsa_private_key(private_key_base64: bytes):
    private_key_bytes = base64.b64decode(private_key_base64)
    private_key = rsa.PrivateKey.load_pkcs1(keyfile=private_key_bytes)
    
    return private_key
