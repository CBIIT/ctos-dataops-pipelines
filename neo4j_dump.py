import paramiko
import subprocess
import argparse
import os
from neo4j_summary import uplaod_s3, process_arguments
from bento.common.s3 import upload_log_file
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME, get_host

#host = get_host(neo4j_uri)

is_shell = True
NEO4J_URI = "neo4j_uri"
NEO4J_USER = "neo4j_user"
NEO4J_PASSWORD = "neo4j_password"
DUMP_FILE = "dump_file_name"
S3_FOLDER = "s3_folder"
S3_BUCKET = "s3_bucket"
argument_list = [NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD, DUMP_FILE, S3_FOLDER, S3_BUCKET]
TMP = "tmp"

if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Neo4j_Dump_Generator'
os.environ[APP_NAME] = 'Neo4j_Dump_Generator'


def parse_arguments():
    parser = argparse.ArgumentParser(description='Generate neo4j database summary')
    parser.add_argument('config_file', help='Confguration file', nargs='?', default=None)
    parser.add_argument('--neo4j-uri', help='The neo4j uri address')
    parser.add_argument('--neo4j-user', help='The neo4j user')
    parser.add_argument('--neo4j-password', help='The neo4j password')
    parser.add_argument('--s3-bucket', help='The upload s3 file bucket')
    parser.add_argument('--s3-folder', help='The upload s3 file folder')
    parser.add_argument('--dump-file-name', help='The neo4j dump file name')
    return parser.parse_args()

def uplaod_s3(config_data, log, file_key):
    # Upload to s3 bucket
    if S3_BUCKET in config_data.keys():
        if S3_BUCKET is not None:
            if S3_FOLDER in config_data.keys():
                if S3_FOLDER is not None:
                    dest = f"s3://{config_data[S3_BUCKET]}/{config_data[S3_FOLDER]}"
                else:
                    dest = f"s3://{config_data[S3_BUCKET]}"
            else:
                dest = f"s3://{config_data[S3_BUCKET]}"
            try:
                print(dest)
                upload_log_file(dest, file_key)
                log.info(f'Uploading neo4j summary file {os.path.basename(file_key)} succeeded!')
            except Exception as e:
                log.error(e)


def main(args):
    log = get_logger('Neo4j Dump Generator')
    config = process_arguments(args, log, argument_list)
    config_data = config.data
    file_key = os.path.join(TMP, config_data[DUMP_FILE])
    print(file_key)
    host = get_host(config_data[NEO4J_URI])
    command = f"sudo neo4j-admin dump --database=neo4j --to={file_key}"
    if host in ['localhost', '127.0.0.1']:
        subprocess.call(command, shell = is_shell)
    else:
        #cmd_list = ["sudo su - commonsdocker","sudo -i", "systemctl stop neo4j", command, "systemctl start neo4j"]
        cmd_list = ["sudo systemctl stop neo4j", command, "sudo systemctl start neo4j"]
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, username=config_data[NEO4J_USER], password=config_data[NEO4J_PASSWORD])
        for cmd in cmd_list:
            print(cmd)
            stdin, stdout, stderr = client.exec_command(cmd)
            output = stdout.read().decode('utf-8')
            error = stderr.read().decode('utf-8')
            if output:
                log.info(output)
            if error:
                log.error(error)
            
        
        
        client.close()
    
    uplaod_s3(config_data, log, file_key)

if __name__ == '__main__':
    main(parse_arguments())