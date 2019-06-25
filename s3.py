import logging
import os
import boto3
import configparser
import sys

from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from cryptography.fernet import Fernet

import progress

# Config the logger
logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S')

# Read the configuration
config = configparser.ConfigParser()
config.read('config.ini')
#file_directory = config['DEFAULT']['file_directory']
file_threshold = int(config['DEFAULT']['multipart_threshold'])
file_chunksize = int(config['DEFAULT']['multipart_chunksize'])
file_thread = int(config['DEFAULT']['max_concurrency'])
use_thread = config['DEFAULT'].getboolean('use_threads')
bucket_name = config['DEFAULT']['bucket_name']

def main():

    if len(sys.argv) != 4:
        print("Usage: python3 s3.py file-directory s3_folder extension \n");
        print("Usage: python3 s3.py /home/dir encrypted/folder-A/ .txt \n");
        return 1

    file_directory = sys.argv[1]
    s3_folder = sys.argv[2]
    extension = sys.argv[3]
    gather_the_file(file_directory, ext=extension, folder_in_s3=s3_folder)

def upload_to_s3(file_path, bucket_name, object_name):
    
    s3_client = boto3.client('s3')
    MB = 1024 ** 2

    # transfer configuration to set the multipart upload.
    transfer_configuration = TransferConfig(multipart_threshold=file_threshold*MB, multipart_chunksize=file_chunksize*MB, use_threads=use_thread, max_concurrency=5)
    extra_args={'Metadata': {'test-folder': '100mb.pdf'}}
    callback = progress.ProgressPercentage(file_path)

    try:
        logging.info("Try upload to S3 " + object_name)
        response = s3_client.upload_file(file_path, bucket_name, object_name, extra_args, Config=transfer_configuration, Callback=callback)
        logging.info("Upload " + object_name + " to S3 success")
    except ClientError as e:
        logging.info("Error Here")
        logging.error(e)
        return False
    return True

def generate_key():
    print("gen key")

def encrypt_file(file_path, ext):
    
    # Get the key from the file
    file = open('key.key', 'rb')
    key = file.read()
    file.close()

    #Open the file to encrypt
    with open(file_path + ext, 'rb') as file:
        data = file.read()

    fernet = Fernet(key)
    encrypted_data = fernet.encrypt(data) 

    #Write the encrypted file
    with open(file_path + '.enc', 'wb') as file:
        file.write(encrypted_data)

def decrypt_file():
   
    # Get the key from the file
    file = open('key.key', 'rb')
    key = file.read()
    file.close()

    #Open the file to encrypt
    with open('sample-small-file-enc.txt', 'rb') as file:
        data = file.read()

    fernet = Fernet(key)
    decrypted_data = fernet.decrypt(data) 

    #Write the encrypted file
    with open('sample-small-file-decrypt.txt', 'wb') as file:
        file.write(decrypted_data)

def gather_the_file(file_path, ext, folder_in_s3):
    
    # this should be an input
    listdir = os.listdir(file_path)

    # Loop the file endswith 
    for file in listdir:
        if file.endswith(ext):
            
            encrypt_file_name = os.path.splitext(file)[0]
            encrypt_file(file_path + encrypt_file_name, ext)
            encrypted_file = encrypt_file_name + '.enc'
            upload_to_s3(file_path + encrypted_file, bucket_name, folder_in_s3 + encrypted_file)
    
if __name__ == "__main__":
    main()