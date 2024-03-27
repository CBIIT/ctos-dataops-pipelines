
from prefect import flow
from data_model_archiving import model_archiving


@flow(name="data model archiving", log_prints=True)
def data_model_archiving(
        data_model_repo_url,
        data_model_version,
        s3_bucket,
        s3_prefix,
    ):

    print("Start generating data model")
    model_archiving(data_model_repo_url, data_model_version, s3_bucket, s3_prefix)
    print("Finish generating data model")

if __name__ == "__main__":
    # create your first deployment
    data_model_archiving.serve(name="data_model_archiving")
    #stream_file_validator()