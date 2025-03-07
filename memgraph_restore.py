import subprocess
import os
import sys
from neo4j_restore import downlaod_s3
from memgraph_export import remove_information

def memgraph_restore(memgraph_host, memgraph_port, memgraph_username, memgraph_password, tmp_folder, s3_bucket, s3_prefix, export_filename, log):
    try:
        restore_file_key = os.path.join(tmp_folder, export_filename)
        s3_file_key = os.path.join(s3_prefix, export_filename)
        download_succeeded = downlaod_s3(s3_bucket, s3_file_key, log, restore_file_key)
        if download_succeeded:
            mgconsole_string = f"mgconsole --host {memgraph_host} --port {memgraph_port} --username {memgraph_username} --password {memgraph_password}"
            command_delete = [
                "sh",
                "-c",
                f'echo "MATCH (n) DETACH DELETE n;"' + " | " + mgconsole_string
                ]
            command_restore = [
                "sh",
                "-c",
                mgconsole_string + f" < {restore_file_key}"]
            commands = [command_delete, command_restore]
            for command in commands:
                result = subprocess.run(command, capture_output=True, text=True)
                #log.info(result)
            log.info(f"Successsfuly restore the Memgraph data from {export_filename}")
    except Exception as e:
        updated_error_message = remove_information(str(e), [memgraph_password, memgraph_username, memgraph_port])
        log.error(updated_error_message)
        sys.exit(1)

