import paramiko
import subprocess
import os
from bento.common.s3 import upload_log_file
from bento.common.utils import get_logger, LOG_PREFIX, APP_NAME, get_host



TMP = "/tmp/"
NOT_ALLOW = "not allowed to execute"
if LOG_PREFIX not in os.environ:
    os.environ[LOG_PREFIX] = 'Neo4j_Dump_Generator'
os.environ[APP_NAME] = 'Neo4j_Dump_Generator'

def uplaod_s3(s3_bucket, s3_folder, log, file_key):
    # Upload to s3 bucket
    dest = f"s3://{s3_bucket}/{s3_folder}"
    try:
        print(dest)
        upload_log_file(dest, file_key)
        log.info(f'Uploading neo4j summary file {os.path.basename(file_key)} succeeded!')
    except Exception as e:
        log.error(e)

def neo4j_dump(dump_file, neo4j_uri, neo4j_user, neo4j_password, s3_bucket, s3_folder):
    dump_fail = False
    is_shell = True
    log = get_logger('Neo4j Dump Generator')
    file_key = os.path.join(TMP, dump_file)
    host = get_host(neo4j_uri)
    #host = neo4j_uri
    command = f"sudo neo4j-admin dump --database=neo4j --to={file_key}"
    if host in ['localhost', '127.0.0.1']:
        try:
            subprocess.call(command, shell = is_shell)
        except Exception as e:
            dump_fail = True
            log.error(e)
    else:
        #cmd_list = ["sudo su - commonsdocker","sudo -i", "systemctl stop neo4j", command, "systemctl start neo4j"]
        cmd_list = ["sudo systemctl stop neo4j", command, "sudo systemctl start neo4j"]
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(host, username=neo4j_user, password=neo4j_password, timeout=30)
        log.info("Connect to the remote server successfully")
        #channel = client.invoke_shell()
        ''''''
        
        try:
            for cmd in cmd_list:
                #print(cmd)
                stdin, stdout, stderr = client.exec_command(cmd, get_pty=True)
                # Provide password for sudo
                stdin.write(neo4j_password + '\n')
                stdin.flush()
                # Read and print command output
                output = stdout.read().decode()
                if neo4j_password in output:
                    output = output.replace(neo4j_password, "password")
                log.info(output)
                if NOT_ALLOW in output:
                    dump_fail = True
                error = stderr.read().decode()
                if error:
                    dump_fail = True
                    log.error("Error occurred:", error)
        except Exception as e:
            dump_fail = True
            log.error(e)
        # Download the file
        if not dump_fail:
            sftp = client.open_sftp()
            sftp.get(file_key, dump_file)
        else:
            log.error("Can not create neo4j dump file")

        client.close()
    if not dump_fail:
        uplaod_s3(s3_bucket, s3_folder, log, file_key)