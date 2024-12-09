import subprocess
import os
import sys
from neo4j_restore import downlaod_s3

def memgraph_restore(memgraph_host, memgraph_port, memgraph_username, memgraph_password, tmp_folder, s3_bucket, s3_prefix, export_filename, log):
    try:
        restore_file_key = os.path.join(tmp_folder, export_filename)
        s3_file_key = os.path.join(s3_prefix, export_filename)
        download_succeeded = False
        download_succeeded = downlaod_s3(s3_bucket, s3_file_key, log, restore_file_key)
        if download_succeeded:
            mgconsole_string = f" | mgconsole --host {memgraph_host} --port {memgraph_port} --username {memgraph_username} --password {memgraph_password}"
            command = [
                "sh",
                "-c",
                f'echo "MATCH (n) DETACH DELETE n;"' + mgconsole_string,
                f'cat {restore_file_key}' + mgconsole_string
                ]
            subprocess.run(command)
    except Exception as e:
        log.error(e)
        sys.exit(1)

