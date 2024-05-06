from prefect import flow
from bento.common.secret_manager import get_secret
from neo4j_dump import neo4j_dump

NEO4J_IP = "neo4j_ip"
NEO4J_USER = "neo4j_user"
NEO4J_KEY = "neo4j_key"

@flow(name="neo4j dump", log_prints=True)
def neo4j_dump_prefect(
        secret_name_ssh,
        secret_name,
        s3_bucket,
        s3_folder,
        dump_file_name = "test_dump.dump"
        
):
    secret = get_secret(secret_name)
    secret_ssh = get_secret(secret_name_ssh)
    neo4j_ip = secret[NEO4J_IP]
    neo4j_user = secret_ssh[NEO4J_USER]
    neo4j_key = secret_ssh[NEO4J_KEY]
    neo4j_dump(dump_file_name, neo4j_ip, neo4j_user, neo4j_key, s3_bucket, s3_folder)

if __name__ == "__main__":
    # create your first deployment
    neo4j_dump_prefect.serve(name="neo4j_dump")

