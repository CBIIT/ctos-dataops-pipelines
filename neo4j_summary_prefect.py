from prefect import flow
from neo4j_summary import neo4j_summary
from bento.common.secret_manager import get_secret
from prefect.artifacts import create_markdown_artifact

NEO4J_IP = "neo4j_ip"
NEO4J_USER = "neo4j_user"
NEO4J_PASSWORD = "neo4j_password"

def create_mark_down(neo4j_dict):
    summary_str = f'''
## Counts by Total

| Counting Type | Counts |
|:--------------|-------:|
| total_ndoes | {neo4j_dict["total_nodes"]:,} |
| total_relationships | {neo4j_dict["total_relationships"]:,} |
'''
    summary_str += f'''
## Counts by Node

| Node Type | Counts |
|:--------------|-------:|
'''
    for node in neo4j_dict["node_counts"].keys():
        node_count_string = f'''| {node} | {neo4j_dict["node_counts"][node]:,} |
'''
        summary_str += node_count_string
    summary_str += f'''## Counts by Relationship

| Relationship Type | Counts |
|:--------------|-------:|
'''
    for relationship in neo4j_dict["relationship_counts"].keys():
        node_count_string = f'''| {relationship} | {neo4j_dict["relationship_counts"][relationship]:,} |
'''
        summary_str += node_count_string
    return summary_str


@flow(name="neo4j secret summary", log_prints=True)
def neo4j_secret_summary_prefect(
        secret_name,
        s3_bucket,
        s3_folder,
        neo4j_summary_file_name = "neo4j_summary.json"
):
    secret = get_secret(secret_name)
    neo4j_ip = secret[NEO4J_IP]
    neo4j_user = secret[NEO4J_USER]
    neo4j_password = secret[NEO4J_PASSWORD]
    print("Start generating neo4j database summary")
    neo4j_dict = neo4j_summary(neo4j_ip, neo4j_user, neo4j_password, neo4j_summary_file_name, s3_bucket, s3_folder)
    summary_md = create_mark_down(neo4j_dict)
    create_markdown_artifact(
        key="neo4j-summary",
        markdown=summary_md,
        description="Neo4j Summary",
    )
    print("Finish generating neo4j database summary")
    '''
    neo4j_summary_prefect(
        neo4j_ip,
        neo4j_user,
        neo4j_password,
        s3_bucket,
        s3_folder,
        neo4j_summary_file_name
    )'''


@flow(name="neo4j summary", log_prints=True)
def neo4j_summary_prefect(
        neo4j_ip,
        neo4j_user,
        neo4j_password,
        s3_bucket,
        s3_folder,
        neo4j_summary_file_name = "neo4j_summary.json"
    ):
    print("Start generating neo4j database summary")
    neo4j_dict = neo4j_summary(neo4j_ip, neo4j_user, neo4j_password, neo4j_summary_file_name, s3_bucket, s3_folder)
    summary_md = create_mark_down(neo4j_dict)
    create_markdown_artifact(
        key="neo4j-summary",
        markdown=summary_md,
        description="Neo4j Summary",
    )
    print("Finish generating neo4j database summary")

if __name__ == "__main__":
    # create your first deployment
    neo4j_summary_prefect.serve(name="neo4j_summary")