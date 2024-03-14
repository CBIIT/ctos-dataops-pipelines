from prefect import flow
from neo4j_summary import main
from bento.common.secret_manager import get_secret

NEO4J_URI = "neo4j_uri"
NEO4J_USER = "neo4j_user"
NEO4J_PASSWORD = "neo4j_password"
 
@flow(name="neo4j secret summary", log_prints=True)
def neo4j_secret_summary(
        secret_name,
        s3_bucket,
        s3_folder,
        neo4j_summary_file_name = "neo4j_summary.json"
):
    secret = get_secret(secret_name)
    neo4j_uri = secret[NEO4J_URI]
    neo4j_user = secret[NEO4J_USER]
    neo4j_password = secret[NEO4J_PASSWORD]
    neo4j_summary(
        neo4j_uri,
        neo4j_user,
        neo4j_password,
        s3_bucket,
        s3_folder,
        neo4j_summary_file_name
    )


@flow(name="neo4j summary", log_prints=True)
def neo4j_summary(
        neo4j_uri,
        neo4j_user,
        neo4j_password,
        s3_bucket,
        s3_folder,
        neo4j_summary_file_name = "neo4j_summary.json"
    ):

    params = Config(
        neo4j_uri,
        neo4j_user,
        neo4j_password,
        s3_bucket,
        s3_folder,
        neo4j_summary_file_name
    )
    print("Start generating neo4j database summary")
    main(params)
    print("Finish generating neo4j database summary")

class Config:
    def __init__(
            self,
            neo4j_uri,
            neo4j_user,
            neo4j_password,
            s3_bucket,
            s3_folder,
            neo4j_summary_file_name
    ):
        
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password
        self.s3_bucket = s3_bucket
        self.s3_folder = s3_folder
        self.neo4j_summary_file_name = neo4j_summary_file_name
        self.config_file = None
        

if __name__ == "__main__":
    # create your first deployment
    neo4j_summary.serve(name="neo4j_deployment")
    #stream_file_validator()