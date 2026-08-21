import argparse
import boto3
import requests
from requests_aws4auth import AWS4Auth
import time

def getArgs():

  parser = argparse.ArgumentParser(description='Opensearch Backup Script')
  parser.add_argument("--oshost", type=str, help="opensearch host with trailing /")
  parser.add_argument("--repo", type=str, help="opensearch snapshot repository")
  parser.add_argument("--s3bucket", type=str, help="s3 bucket")
  parser.add_argument("--snapshot", type=str, help="opensearch snapshot value")
  parser.add_argument("--indices", type=str, help="indices", nargs='?', const='')
  parser.add_argument("--rolearn", type=str, help="role arn - typically power user role")
  parser.add_argument("--basepath", type=str, help="basepath", nargs='?', const='')
  parser.add_argument("--region", type=str, help="region")
  args = parser.parse_args()
  
  argList = {}
  argList['oshost'] = args.oshost
  argList['repo'] = args.repo
  argList['s3bucket'] = args.s3bucket
  argList['snapshot'] = args.snapshot 
  argList['indices'] = args.indices
  argList['rolearn'] = args.rolearn
  argList['region'] = args.region

  basepath = args.basepath
  if basepath :
    argList['basepath'] = basepath + '/' + argList['snapshot']
  else:
    argList['basepath'] = argList['snapshot']

  return argList


def osSession(argList):
  # Cross-account access requires assuming a role in the OpenSearch/S3 account.
  operations_role = argList.get('operationsrolearn')
  if not operations_role:
    return boto3.Session()

  print(f"assuming operations role {operations_role}")
  assumed = boto3.client('sts').assume_role(
    RoleArn=operations_role,
    RoleSessionName='PrefectOpenSearchSession'
  )['Credentials']

  return boto3.Session(
    aws_access_key_id=assumed['AccessKeyId'],
    aws_secret_access_key=assumed['SecretAccessKey'],
    aws_session_token=assumed['SessionToken']
  )


def osAuth(argList):
  # Opensearch authentication
  service = 'es'
  credentials = osSession(argList).get_credentials()
  awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, argList['region'], service, session_token=credentials.token)

  return awsauth


def registerRepo(argList, awsauth):

  # Registering Repo
  path = '_snapshot/' + argList['repo']
  url = argList['oshost'] + path

  payload = {
    "type": "s3",
    "settings": {
      "bucket": argList['s3bucket'],
      "base_path": argList['basepath'],
      "region": argList['region'],
      "role_arn": argList['rolearn'],
      "canned_acl": "bucket-owner-full-control"
    }
  }

  headers = {"Content-Type": "application/json"}
  print("registering repo")
  try:
    r = requests.put(url, auth=awsauth, json=payload, headers=headers)
  except requests.exceptions.RequestException as e:
    raise SystemExit(e)

  print(f"repository registration returned {r.status_code}: {r.text}")
  if not r.ok:
    raise Exception(
      f"Unable to register snapshot repository '{argList['repo']}': "
      f"HTTP {r.status_code}: {r.text}"
    )

  repository = requests.get(url, auth=awsauth, headers=headers)
  if not repository.ok or argList['repo'] not in repository.json():
    raise Exception(
      f"Snapshot repository '{argList['repo']}' was not available after registration: "
      f"HTTP {repository.status_code}: {repository.text}"
    )


def deleteIndexes(argList, awsauth):
  # Deleting Indexes
  headers = {"Content-Type": "application/json"}
  
  if argList['indices']:
    print("deleting the listed indices")
    indice_arr = argList['indices'].split(",")
    for i in indice_arr:
      check = requests.get(argList['oshost'] + i, auth=awsauth, headers=headers)
      if check.status_code==200:
        try:
          r = requests.delete(argList['oshost'] + i, auth=awsauth, headers=headers)
          print(r.text)
        except requests.exceptions.RequestException as e:
          raise SystemExit(e)
  else:
    print("no listed indices - deleting all indices")
    try:
      r = requests.delete(argList['oshost'] + '*', auth=awsauth, headers=headers)
      print(r.text)
    except requests.exceptions.RequestException as e:
     raise SystemExit(e)

  print("finished deleting the indices, waiting 2 mins for the deletion to complete")
  time.sleep(120)


def restoreIndexes(argList, awsauth):

  # Restoring Indexes
  print("started restore the indices")
  
  # Create Index list to exclude hidden (default) indices
  if argList['indices']:
    print("setting restore to use listed indices")
    indices = '-.*,' + argList['indices']
  else:
    print("setting restore to use all indices")
    indices = '*,-.*'
  
  headers = {"Content-Type": "application/json"}

  payload = {
    "indices": indices,
    "include_global_state": False,
  }
  path = '_snapshot/' + argList['repo'] + '/' + argList['snapshot'] + '/_restore'
  print(argList['oshost'] + path, payload)
  try:
    result = requests.post(argList['oshost'] + path, auth=awsauth, json=payload, headers=headers)
  except requests.exceptions.RequestException as e:
     raise SystemExit(e)

  return result


if __name__ == "__main__":
   argList = getArgs()
   awsauth = osAuth(argList)
   registerRepo(argList, awsauth)

   deleteIndexes(argList, awsauth)
   result = restoreIndexes(argList, awsauth)
   print(result.text)
   if result.status_code!=200:
    raise Exception("Sorry, pipeline does not run successfully")

def opensearch_restore(argList):
    awsauth = osAuth(argList)
    registerRepo(argList, awsauth)

    deleteIndexes(argList, awsauth)
    result = restoreIndexes(argList, awsauth)
    print(result.text)
    if result.status_code!=200:
        raise Exception("Sorry, pipeline does not run successfully")