import io
import requests
import os
from requests_kerberos import HTTPKerberosAuth

IDBROKER_HOST_NAME = os.environ["IDBROKER_HOST_NAME"]
IDBROKER_HOST_PORT = "8444"
token_url = f"https://{IDBROKER_HOST_NAME}:{IDBROKER_HOST_PORT}/gateway/dt/knoxtoken/api/v1/token"
url =  f"https://{IDBROKER_HOST_NAME}:{IDBROKER_HOST_PORT}/gateway/aws-cab/cab/api/v1/credentials"
r = requests.get(token_url, auth=HTTPKerberosAuth())
headers = {
    'Authorization': "Bearer "+ r.json()['access_token'],
    'cache-control': "no-cache"
    }

response = requests.request("GET", url, headers=headers)
os.environ["AWS_ACCESS_KEY_ID"]=response.json()['Credentials']['AccessKeyId']
os.environ["AWS_SECRET_ACCESS_KEY"]=response.json()['Credentials']['SecretAccessKey']
os.environ["AWS_DEFAULT_REGION"]='us-east-1'
os.environ["AWS_SESSION_TOKEN"]=response.json()['Credentials']['SessionToken']
ACCESS_KEY=os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY=os.environ["AWS_SECRET_ACCESS_KEY"]
SESSION_TOKEN=os.environ["AWS_SESSION_TOKEN"]
REGION_NAME=os.environ["AWS_DEFAULT_REGION"]