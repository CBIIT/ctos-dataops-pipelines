# import argparse
import boto3
import requests
# from requests_aws4auth import AWS4Auth
from urllib.parse import urljoin
import time

from opensearch_utils import sigv4_request

# def getArgs():

#   parser = argparse.ArgumentParser(description='Opensearch Restore Script')
#   parser.add_argument("--oshost", type=str, help="opensearch host with trailing /")
#   parser.add_argument("--repo", type=str, help="opensearch snapshot repository")
#   parser.add_argument("--s3bucket", type=str, help="s3 bucket")
#   parser.add_argument("--snapshot", type=str, help="opensearch snapshot value")
#   parser.add_argument("--indices", type=str, help="indices", nargs='?', const='')
#   parser.add_argument("--rolearn", type=str, help="role arn - typically power user role")
#   parser.add_argument("--osrolearn", type=str, help="opensearch role arn")
#   parser.add_argument("--basepath", type=str, help="basepath", nargs='?', const='')
#   parser.add_argument("--region", type=str, help="region")
#   args = parser.parse_args()
  
#   argList = {}
#   argList['oshost'] = args.oshost
#   argList['repo'] = args.repo
#   argList['s3bucket'] = args.s3bucket
#   argList['snapshot'] = args.snapshot 
#   argList['indices'] = args.indices
#   argList['rolearn'] = args.rolearn
#   argList['osrolearn'] = args.osrolearn
#   argList['region'] = args.region

#   basepath = args.basepath
#   if basepath :
#     argList['basepath'] = basepath + '/' + argList['snapshot']
#   else:
#     argList['basepath'] = argList['snapshot']

#   return argList


# def osAuth(argList):
#   # Opensearch authentication
#   service = 'es'
#   credentials = boto3.Session().get_credentials()
#   awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, argList['region'], service, session_token=credentials.token)

#   return awsauth


def assume_role(role_arn: str, session_name: str = "snapshot-ops", external_id: str | None = None):
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

  # Registering Repo
  # path = '_snapshot/' + argList['repo']
  # url = argList['oshost'] + path

  # payload = {
  #   "type": "s3",
  #   "settings": {
  #     "bucket": argList['s3bucket'],
  #     "base_path": argList['basepath'],
  #     "region": argList['region'],
  #     "role_arn": argList['rolearn'],
  #     "canned_acl": "bucket-owner-full-control"
  #   }
  # }

  # headers = {"Content-Type": "application/json"}
  # print("registering repo")
  # try:
  #   r = requests.put(url, auth=awsauth, json=payload, headers=headers)
  #   time.sleep(100)
  #   print(r.text)
  # except requests.exceptions.RequestException as e:
  #   raise SystemExit(e)

  # === Example: register a repository (PUT _snapshot/<repo>) ===
  # repo_name = argList['repo']
  url = urljoin(argList['oshost'].rstrip("/") + "/_snapshot/" + argList['repo'])
  body = {
      "type": "s3",
      "settings": {
          "bucket": argList['s3bucket'],
          "base_path": argList['basepath'],
          "region": argList['region'],
          "role_arn": argList['rolearn'],
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
  # Deleting Indexes
  # headers = {"Content-Type": "application/json"}
  
  # if argList['indices']:
  #   print("deleting the listed indices")
  #   indice_arr = argList['indices'].split(",")
  #   for i in indice_arr:
  #     check = requests.get(argList['oshost'] + i, auth=awsauth, headers=headers)
  #     if check.status_code==200:
  #       try:
  #         r = requests.delete(argList['oshost'] + i, auth=awsauth, headers=headers)
  #         print(r.text)
  #       except requests.exceptions.RequestException as e:
  #         raise SystemExit(e)
  # else:
  #   print("no listed indices - deleting all indices")
  #   try:
  #     r = requests.delete(argList['oshost'] + '*', auth=awsauth, headers=headers)
  #     print(r.text)
  #   except requests.exceptions.RequestException as e:
  #    raise SystemExit(e)

  if argList['indices']:
    print("deleting the listed indices")
    indice_arr = argList['indices'].split(",")
    for i in indice_arr:
      # check = requests.get(argList['oshost'] + i, auth=awsauth, headers=headers)
      url = urljoin(argList['oshost'] + i)
      check = sigv4_request(
        session=assumed_sess,
        region=argList['region'],
        service="es",
        method="GET",
        url=url,
        headers={"Content-Type": "application/json"},
      )
      # print(check.status_code)
      # print(check.text)
      if check.status_code==200:
        try:
          # r = requests.delete(argList['oshost'] + i, auth=awsauth, headers=headers)
          # print(r.text)
          url = urljoin(argList['oshost'] + i)
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
        except requests.exceptions.RequestException as e:
          raise SystemExit(e)
  else:
    print("no listed indices - deleting all indices")
    try:
      # r = requests.delete(argList['oshost'] + '*', auth=awsauth, headers=headers)
      # print(r.text)
      url = urljoin(argList['oshost'] + '*')
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
    except requests.exceptions.RequestException as e:
      raise SystemExit(e)

  print("finished deleting the indices, waiting 2 mins for the deletion to complete")
  time.sleep(120)


def restoreIndexes(argList, assumed_sess):

  # Restoring Indexes
  print("started restore the indices")
  
  # Create Index list to exclude hidden (default) indices
  if argList['indices']:
    print("setting restore to use listed indices")
    indices = '-.*,' + argList['indices']
  else:
    print("setting restore to use all indices")
    indices = '*,-.*'
  
  # headers = {"Content-Type": "application/json"}

  # payload = {
  #   "indices": indices,
  #   "include_global_state": False,
  # }
  # path = '_snapshot/' + argList['repo'] + '/' + argList['snapshot'] + '/_restore'
  # print(argList['oshost'] + path, payload)
  # try:
  #   result = requests.post(argList['oshost'] + path, auth=awsauth, json=payload, headers=headers)
  # except requests.exceptions.RequestException as e:
  #    raise SystemExit(e)

  # === Register snapshot repository ===
  # repo_name = argList['repo']
  body = {
    "indices": indices,
    "include_global_state": False,
  }
  url = urljoin(argList['oshost'].rstrip("/") + "/_snapshot/" + argList['repo'] + "/" + argList['snapshot'] + "/_restore")
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
  # print(result.text)

  return result


# if __name__ == "__main__":
#   argList = getArgs()
#   #  awsauth = osAuth(argList)
#      # === Assume into 265 ===
#   assumed_sess = assume_role(
#       role_arn=repo_admin_role_arn,
#       session_name="repo-admin-snapshot-ops",
#       # external_id=external_id,
#       external_id=None,
#   )
#   registerRepo(argList, awsauth)

#   deleteIndexes(argList, awsauth)
#   result = restoreIndexes(argList, awsauth)
#   print(result.text)
#   if result.status_code!=200:
#    raise Exception("Sorry, pipeline does not run successfully")

def opensearch_restore(argList):
    # awsauth = osAuth(argList)
    assumed_sess = assume_role(
      role_arn=argList['rolearn'],
      session_name="repo-admin-snapshot-ops",
      # external_id=external_id,
      external_id=None,
    )
    registerRepo(argList, assumed_sess)

    deleteIndexes(argList, assumed_sess)
    result = restoreIndexes(argList, assumed_sess)
    print(result.text)
    if result.status_code!=200:
      raise Exception("Sorry, pipeline does not run successfully")