from cryptography.fernet import Fernet

key = Fernet.generate_key()

file = open('key.key', 'wb')
file.write(key) # the key is type bytes
file.close()