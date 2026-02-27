import boto3
from urllib.parse import urljoin
import time

from opensearch_utils import sigv4_request
from typing import Optional, Dict, Any


def assume_role(role_arn: str, session_name: str = "snapshot-ops", external_id: Optional[Dict[str, Any]] = None):
    """Assume a role and return a boto3.Session using the temporary creds."""
    sts = boto3.client("sts")
    params = {"RoleArn": role_arn, "RoleSessionName": session_name}
    if external_id:
        params["ExternalId"] = external_id

    resp = sts.assume_role(**params)
    c = resp["Credentials"]

    return boto3.Session(
        aws_access_key_id=c["AccessKeyId"],
        aws_secret_access_key=c["SecretAccessKey"],
        aws_session_token=c["SessionToken"],
    )


def registerRepo(argList, assumed_sess):
  url = urljoin(argList['oshost'].rstrip("/") + "/_snapshot/", argList['repo'])
  body = {
      "type": "s3",
      "settings": {
          "bucket": argList['s3bucket'],
          "base_path": argList['basepath'],
          "region": argList['region'],
          "role_arn": argList['osrolearn'],
          "canned_acl": "bucket-owner-full-control"
      }
  }
  r = sigv4_request(
      session=assumed_sess,
      region=argList['region'],
      service="es",
      method="PUT",
      url=url,
      body=body,
      headers={"Content-Type": "application/json"},
  )
  print(r.status_code, r.text)


def deleteIndexes(argList, assumed_sess):
  if argList['indices']:
    print("deleting the listed indices")
    indice_arr = argList['indices'].split(",")
    for i in indice_arr:
      url = urljoin(argList['oshost'], i)
      check = sigv4_request(
        session=assumed_sess,
        region=argList['region'],
        service="es",
        method="GET",
        url=url,
        headers={"Content-Type": "application/json"},
      )
      if check.status_code==200:
        url = urljoin(argList['oshost'], i)
        r = sigv4_request(
          session=assumed_sess,
          region=argList['region'],
          service="es",
          method="DELETE",
          url=url,
          headers={"Content-Type": "application/json"},
        )
        print(r.status_code)
        print(r.text)
  else:
    print("no listed indices - deleting all indices")
    url = urljoin(argList['oshost'], '*')
    r = sigv4_request(
      session=assumed_sess,
      region=argList['region'],
      service="es",
      method="DELETE",
      url=url,
      headers={"Content-Type": "application/json"},
    )
    print(r.status_code)
    print(r.text)

  print("finished deleting the indices, waiting 2 mins for the deletion to complete")
  time.sleep(120)


def restoreIndexes(argList, assumed_sess):
  print("started restore the indices")
  
  # Create Index list to exclude hidden (default) indices
  if argList['indices']:
    print("setting restore to use listed indices")
    indices = '-.*,' + argList['indices']
  else:
    print("setting restore to use all indices")
    indices = '*,-.*'

  body = {
    "indices": indices,
    "include_global_state": False,
  }
  url = urljoin(argList['oshost'].rstrip("/") + "/_snapshot/", argList['repo'] + "/" + argList['snapshot'] + "/_restore")
  result = sigv4_request(
    session=assumed_sess,
    region=argList['region'],
    service="es",
    method="POST",
    url=url,
    body=body,
    headers={"Content-Type": "application/json"},
  )
  print(result.status_code)
  return result


def opensearch_restore(argList):
    assumed_sess = assume_role(
      role_arn=argList['rolearn'],
      session_name="repo-admin-snapshot-ops",
      external_id=None,
    )
    print(assumed_sess.client("sts").get_caller_identity())
    registerRepo(argList, assumed_sess)

    deleteIndexes(argList, assumed_sess)
    result = restoreIndexes(argList, assumed_sess)
    print(result.text)
    if result.status_code!=200:
      raise Exception("Sorry, pipeline does not run successfully")