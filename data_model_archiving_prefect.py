
from prefect import flow
from data_model_archiving import main


@flow(name="data model archiving", log_prints=True)
def data_model_archiving(
        data_model_repo_url,
        data_model_version,
        s3_bucket,
        s3_prefix,
    ):

    params = Config(
        data_model_repo_url,
        data_model_version,
        s3_bucket,
        s3_prefix,
    )
    print("Start generating neo4j database summary")
    main(params)
    print("Finish generating neo4j database summary")

class Config:
    def __init__(
            self,
            data_model_repo_url,
            data_model_version,
            s3_bucket,
            s3_prefix,
    ):
        
        self.data_model_repo_url = data_model_repo_url
        self.data_model_version = data_model_version
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.config_file = None
        

if __name__ == "__main__":
    # create your first deployment
    data_model_archiving.serve(name="data_model_archiving")
    #stream_file_validator()