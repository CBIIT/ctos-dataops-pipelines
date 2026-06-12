import argparse
import boto3
import time
from urllib.parse import urljoin
from typing import Optional, Dict, Any

from opensearch_utils import sigv4_request


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


def getArgs():
  parser = argparse.ArgumentParser(description='Opensearch Backup Script')
  parser.add_argument("--oshost", type=str, help="opensearch host with trailing /")
  parser.add_argument("--repo", type=str, help="opensearch snapshot repository")
  parser.add_argument("--s3bucket", type=str, help="s3 bucket")
  parser.add_argument("--snapshot", type=str, help="opensearch snapshot value")
  parser.add_argument("--indices", type=str, help="indices", nargs='?', const='')
  parser.add_argument("--basepath", type=str, help="basepath", nargs='?', const='')
  parser.add_argument("--region", type=str, help="region")
  args = parser.parse_args()

  argList = {}
  argList['oshost'] = args.oshost
  argList['repo'] = args.repo
  argList['s3bucket'] = args.s3bucket
  argList['snapshot'] = args.snapshot 
  argList['indices'] = args.indices
  argList['region'] = args.region

  basepath = args.basepath
  if basepath :
    argList['basepath'] = basepath + '/' + argList['snapshot']
  else:
    argList['basepath'] = argList['snapshot']

  return argList


def check_repository(argList, session):
    check_url = urljoin(argList['oshost'].rstrip("/") + "/_snapshot/", "_all")
    print("checking repo")
    print(check_url)
    response = sigv4_request(
        session=session,
        region=argList['region'],
        service="es",
        method="GET",
        url=check_url,
        headers={"Content-Type": "application/json"},
    )
    repos = response.json()
    for repo_name, details in repos.items():
        print(f"- {repo_name}: {details}")
    return response.status_code == 200

def registerRepo(argList, session):

  # Registering Repo
  url = urljoin(argList['oshost'].rstrip("/") + "/_snapshot/", argList['repo'])

  payload = {
    "type": "s3",
    "settings": {
      "bucket": argList['s3bucket'],
      "base_path": argList['basepath'],
      "region": argList['region'],
      "role_arn": argList['osrolearn'],
      "canned_acl": "bucket-owner-full-control"
    }
  }

  print("registering repo")
  print("herere payload url " + str(payload) + "url" + url)
  r = sigv4_request(
      session=session,
      region=argList['region'],
      service="es",
      method="PUT",
      url=url,
      body=payload,
      headers={"Content-Type": "application/json"},
  )
  time.sleep(5)
  print(r.text)
  

def createSnapshot(argList, session):
  # Create Index list to exclude hidden (default) indices
  if argList['indices']:
    print("setting backup to use listed indices")
    indices = '-.*,' + argList['indices']
  else:
    print("setting backup to use all indices")
    indices = '*,-.*'
  
  # Create Snapshot
  snapshot_url = urljoin(
    argList['oshost'].rstrip("/") + "/_snapshot/",
    argList['repo'] + '/' + argList['snapshot'] + '/'
  )

  payload = {
    "indices": indices,
    "include_global_state": False
  }

  print("taking opensearch snapshot")
  print(snapshot_url, payload)
  result = sigv4_request(
      session=session,
      region=argList['region'],
      service="es",
      method="PUT",
      url=snapshot_url,
      body=payload,
      headers={"Content-Type": "application/json"},
  )

  return result


if __name__ == "__main__":
   argList = getArgs()
   session = boto3.Session()
   registerRepo(argList, session)

   result = createSnapshot(argList, session)
   print(result.text)
   if result.status_code!=200:
    raise Exception("Sorry, pipeline does not run successfully")

# entrance for Prefect
def opensearch_backup(argList):
    # Assume role if provided
    if 'rolearn' in argList and argList['rolearn']:
        print(f"Assuming role: {argList['rolearn']}")
        session = assume_role(argList['rolearn'])
    else:
        session = boto3.Session()
    
    print(session.client("sts").get_caller_identity())
    registerRepo(argList, session)
    check_repository(argList, session)

    result = createSnapshot(argList, session)
    print(result.text)
    if result.status_code!=200:
        raise Exception("Sorry, pipeline does not run successfully")