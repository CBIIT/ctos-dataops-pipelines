from prefect import flow
from neo4j_summary import neo4j_summary
from bento.common.secret_manager import get_secret
from prefect.artifacts import create_table_artifact

NEO4J_IP = "neo4j_ip"
NEO4J_USER = "neo4j_user"
NEO4J_PASSWORD = "neo4j_password"

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
    neo4j_summary_prefect(
        neo4j_ip,
        neo4j_user,
        neo4j_password,
        s3_bucket,
        s3_folder,
        neo4j_summary_file_name
    )

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
    general_counting = [{"counting_type": "total_nodes", "counts": neo4j_dict["total_nodes"]},
                        {"counting_type": "total_relationships", "counts": neo4j_dict["total_relationships"]}]
    node_counts = []
    for node in neo4j_dict["node_counts"]:
        node_counts.append({"node":node, "counts": neo4j_dict["node_counts"][node]})
    relationship_counts = []
    for relationship in neo4j_dict["relationship_counts"]:
        relationship_counts.append({"relationship":relationship, "counts": neo4j_dict["relationship_counts"][relationship]})
    create_table_artifact(
        key="total-counts",
        table=general_counting
    )
    create_table_artifact(
        key="node-counts",
        table=node_counts
    )
    create_table_artifact(
        key="relationship-counts",
        table=relationship_counts
    )
    print("Finish generating neo4j database summary")

if __name__ == "__main__":
    # create your first deployment
    neo4j_summary_prefect.serve(name="neo4j_deployment")