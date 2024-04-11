import paramiko
import subprocess
import os
import time
from scp import SCPClient
from bento.common.s3 import upload_log_file
from bento.common.utils import get_logger, get_time_stamp, LOG_PREFIX, APP_NAME
import signal

class TimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutError("Code execution timed out")

def uplaod_s3(s3_bucket, s3_folder, log, file_key):
    # Upload to s3 bucket
    dest = f"s3://{s3_bucket}/{s3_folder}"
    try:
        upload_log_file(dest, file_key)
        log.info(f'Uploading neo4j summary file {os.path.basename(file_key)} succeeded!')
    except Exception as e:
        log.error(e)

def wait_for_complete(log, channel, recv_timeout):
    output_buffer = ""
    signal.signal(signal.SIGALRM, timeout_handler)
    while not output_buffer.endswith("~]$ "):
        try:
            # Attempt to receive data from the channel
            signal.alarm(recv_timeout)
            recv_data = channel.recv(1024).decode()
            signal.alarm(0)
        except TimeoutError as e:
            #log.info("ouput timeout")
            recv_data = ""
        finally:
            signal.alarm(0)
        output_buffer += recv_data
    return output_buffer

def neo4j_dump(dump_file, neo4j_ip, neo4j_user, neo4j_key, s3_bucket, s3_folder):
    neo4j_pem = "neo4j_pem.pem"
    dump_fail = False
    is_shell = True
    TMP = "/tmp/"
    if LOG_PREFIX not in os.environ:
        os.environ[LOG_PREFIX] = 'Neo4j_Dump_Generator'
    os.environ[APP_NAME] = 'Neo4j_Dump_Generator'
    log = get_logger('Neo4j Dump Generator')
    file_key = os.path.join(TMP, dump_file)
    #host = get_host(neo4j_ip)
    host = neo4j_ip
    command = f"sudo neo4j-admin dump --database=neo4j --to={file_key}"
    if host in ['localhost', '127.0.0.1']:
        try:
            subprocess.call(command, shell = is_shell)
        except Exception as e:
            dump_fail = True
            log.error(e)
    else:
        #cmd_list = ["sudo su - commonsdocker","sudo -i", "systemctl stop neo4j", command, "systemctl start neo4j"]
        cmd_list = ["sudo systemctl stop neo4j", command, f"sudo chmod 666 {file_key}", "sudo systemctl start neo4j"]
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        with open(neo4j_pem, 'w') as pem_file:
            pem_file.write(neo4j_key)
        pkey = paramiko.RSAKey.from_private_key_file(neo4j_pem)
        log.info("Start connecting to the neo4j server")
        #client.connect(host, username=neo4j_user, password=neo4j_password, timeout=30)
        client.connect(host, username=neo4j_user, pkey=pkey, timeout=30)
        log.info("Connect to the remote server successfully")
        channel = client.invoke_shell()
        ''''''
        try:
            for cmd in cmd_list:
                channel.send(cmd + "\n")
                while not channel.recv_ready():
                    time.sleep(0.1)
                #set up timer because channel.recv() will stuck when there is no more output
                recv_timeout = 3
                output_buffer = wait_for_complete(log, channel, recv_timeout)
                log.info(output_buffer)
        except Exception as e:
            dump_fail = True
            log.error(e)
        # Download the file
        log.info(f"Start downloading from {file_key}")
        timestamp = get_time_stamp()
        local_file_key = os.path.join('tmp', dump_file.replace(os.path.splitext(dump_file)[1], "_" + timestamp + ".dump"))
        if not dump_fail:
            #download_cmd = f"scp {neo4j_ip}:{file_key} ."
            scp = SCPClient(client.get_transport())
            scp.get(file_key, local_file_key)
        else:
            log.error("Can not create neo4j dump file")

        client.close()
    if not dump_fail:
        log.info(f"Start uploading file {local_file_key} to S3://{s3_bucket}/{s3_folder}")
        uplaod_s3(s3_bucket, s3_folder, log, local_file_key)