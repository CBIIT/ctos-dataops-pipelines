import boto3
import json
from botocore.exceptions import ClientError

def get_secret_ARN(secret_name: str, account: str, region_name: str="us-east-1") -> dict:
        """Returns a dictionary that contains all secret values of secret name using ARN

        Args:
            secret_name (str): A secret name
            region_name (str, optional): Defaults to "us-east-1".

        Raises:
            e: Raise ClientError

        Returns:
            dict: Dictionary of secret_name value
        """
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name, account_id =account)

        try:
            # Build the full ARN for the secret
            arn = build_arn(secret_name, region_name)
            # Retrieve secret using the full ARN
            response = client.get_secret_value(SecretId=arn)
            return json.loads(response["SecretString"])
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise e

def build_arn(secret_name: str, account_id, region_name: str = "us-east-1") -> str:
    """Constructs a full AWS ARN for a Secrets Manager secret

    Args:
        secret_name (str): The name of the secret
        region_name (str, optional): AWS region. Defaults to "us-east-1".
        partition (str, optional): AWS partition. Defaults to "aws".

    Returns:
        str: Full ARN string in format arn:partition:secretsmanager:region:account-id:secret:secret-name
    """
    session = boto3.session.Session()
    sts_client = session.client(service_name="sts", region_name=region_name)
    
    try:
        # account_id = sts_client.get_caller_identity()["Account"]\
        arn = f"arn:aws:secretsmanager:{region_name}:{account_id}:secret:{secret_name}"
        return arn
    except ClientError as e:
        raise e
