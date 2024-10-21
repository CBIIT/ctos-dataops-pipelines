import subprocess
import os
from bento.common.utils import get_time_stamp
from bento.common.s3 import upload_log_file



def upload_s3(s3_prefix, s3_bucket, file_key, log):
    dest = os.path.join(f"s3://{s3_bucket}", s3_prefix)
    upload_log_file(dest, file_key)
    log.info(f'Uploading memgraph export file {os.path.basename(file_key)} succeeded!')


def memgraph_export(memgraph_host, memgraph_port, memgraph_username, memgraph_password, tmp_folder, s3_bucket, s3_prefix, log):
    try:
        export_filename = "memgraph_export-" + get_time_stamp() + ".cypherl"
        export_file_key = os.path.join(tmp_folder, export_filename)
        command = [
            "sh",
            "-c",
            f'echo "DUMP DATABASE;" | mgconsole --host {memgraph_host} --port {memgraph_port} --username {memgraph_username} --password {memgraph_password} --output-format=cypherl > {export_file_key}'
            ]
        subprocess.run(command)
        upload_s3(s3_prefix, s3_bucket, export_file_key, log)
    except Exception as e:
        log.error(e)

