import os
import json
import boto3
import requests
from urllib.parse import urljoin

from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials


# def assume_role(role_arn: str, session_name: str = "snapshot-ops", external_id: str | None = None):
#     """Assume a role and return a boto3.Session using the temporary creds."""
#     sts = boto3.client("sts")
#     params = {"RoleArn": role_arn, "RoleSessionName": session_name}
#     if external_id:
#         params["ExternalId"] = external_id

#     resp = sts.assume_role(**params)
#     c = resp["Credentials"]

#     return boto3.Session(
#         aws_access_key_id=c["AccessKeyId"],
#         aws_secret_access_key=c["SecretAccessKey"],
#         aws_session_token=c["SessionToken"],
#     )


def sigv4_request(
    session: boto3.Session,
    region: str,
    service: str,
    method: str,
    url: str,
    body: dict | None = None,
    headers: dict | None = None,
    timeout: int = 30,
):
    """
    Make a SigV4-signed HTTP request using creds from the provided boto3 session.
    For OpenSearch Service, service is usually 'es'.
    """
    headers = headers or {}
    payload = "" if body is None else json.dumps(body)

    creds = session.get_credentials()
    frozen = creds.get_frozen_credentials()

    aws_creds = Credentials(frozen.access_key, frozen.secret_key, frozen.token)

    # Create and sign the request
    req = AWSRequest(method=method, url=url, data=payload, headers={"Host": requests.utils.urlparse(url).netloc, **headers})
    SigV4Auth(aws_creds, service, region).add_auth(req)

    prepared = req.prepare()

    # Execute
    resp = requests.request(
        method=method,
        url=url,
        data=payload,
        headers=dict(prepared.headers),
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp


# def main():
#     # === CONFIG ===
#     region = os.environ.get("AWS_REGION", "us-east-1")

#     # Role 2 in account 265 (RepoAdminForSnapshots)
#     repo_admin_role_arn = "arn:aws:iam::265135454114:role/RepoAdminForSnapshots"

#     # Your OpenSearch domain endpoint in account 265
#     # Example: https://search-your-domain-abcdefg.us-east-1.es.amazonaws.com/
#     opensearch_endpoint = "https://search-<DOMAIN_NAME>-<ID>.<REGION>.es.amazonaws.com/"

#     # Optional if you enforce ExternalId in the trust policy
#     external_id = None  # "your-external-id"

#     # === Assume into 265 ===
    # assumed_sess = assume_role(
    #     role_arn=repo_admin_role_arn,
    #     session_name="repo-admin-snapshot-ops",
    #     external_id=external_id,
    # )

    # === Example: list snapshot repositories (GET _snapshot) ===
    # url = urljoin(opensearch_endpoint.rstrip("/") + "/", "_snapshot")
    # r = sigv4_request(
    #     session=assumed_sess,
    #     region=region,
    #     service="es",
    #     method="GET",
    #     url=url,
    #     headers={"Content-Type": "application/json"},
    # )
    # print(r.status_code)
    # print(r.text)

    # === Example: register a repository (PUT _snapshot/<repo>) ===
    # repo_name = "my-s3-repo"
    # register_url = urljoin(opensearch_endpoint.rstrip("/") + "/", f"_snapshot/{repo_name}")
    # body = {
    #     "type": "s3",
    #     "settings": {
    #         "bucket": "<SNAPSHOT_BUCKET>",
    #         "base_path": "<PREFIX>",
    #         "region": region,
    #         "role_arn": "arn:aws:iam::265135454114:role/OpenSearchSnapshotRole"
    #     }
    # }
    # r = sigv4_request(
    #     session=assumed_sess,
    #     region=region,
    #     service="es",
    #     method="PUT",
    #     url=register_url,
    #     body=body,
    #     headers={"Content-Type": "application/json"},
    # )
    # print(r.status_code, r.text)


# if __name__ == "__main__":
#     main()