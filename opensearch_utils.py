import json
import boto3
import requests

from botocore.awsrequest import AWSRequest
from botocore.auth import SigV4Auth
from botocore.credentials import Credentials
from typing import Optional, Dict, Any


def sigv4_request(
    session: boto3.Session,
    region: str,
    service: str,
    method: str,
    url: str,
    body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
    raise_for_status: bool = True,
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

    if raise_for_status and not resp.ok:
        print("=== OpenSearch HTTP Error ===")
        print("Status:", resp.status_code)
        print("URL:", resp.url)
        print("Body:", resp.text)
        resp.raise_for_status()

    return resp