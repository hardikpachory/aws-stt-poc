import json
import urllib.request
import urllib.error
import base64
import boto3
import os

# Initialize S3 client
s3_client = boto3.client('s3')


CLIENT_ID = ''
CLIENT_SECRET = ''
USERNAME = ''
PASSWORD = ''
LOGIN_URL = ''


def get_sf_access_token():
    url = f"{LOGIN_URL}/services/oauth2/token"
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    data = {
        'grant_type': 'password',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'username': USERNAME,
        'password': f"{PASSWORD}"
    }

    encoded_data = urllib.parse.urlencode(data).encode('utf-8')

    req = urllib.request.Request(url, data=encoded_data, headers=headers, method='POST')

    try:
        with urllib.request.urlopen(req) as response:
            response_data = json.load(response)
            access_token = response_data['access_token']
            instance_url = response_data['instance_url']
            print(f"Access Token: {access_token} : {instance_url}")
            return access_token, instance_url

    except urllib.error.HTTPError as e:
        error_response = e.read().decode('utf-8')
        print(f"Error getting access token: {error_response}")
        raise Exception(f"Error getting access token: {error_response}")


def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    file_key = event['Records'][0]['s3']['object']['key']
    parentId = file_key.split('-')[2]
    print(f"Bucket Name: {bucket_name} : {file_key}")
    try:
        s3_response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_data = s3_response['Body'].read()

        file_base64 = base64.b64encode(file_data).decode('utf-8')
        
        print(f"First Publish Location : {parentId}")
        
        content_version_payload = {
            "FirstPublishLocationId": parentId,
            'PathOnClient': file_key,
            'VersionData': file_base64
        }

        sf_access_token, instance_url = get_sf_access_token()
        upload_url = f"{instance_url}/services/data/v54.0/sobjects/ContentVersion/"
        headers = {
            'Authorization': f'Bearer {sf_access_token}',
            'Content-Type': 'application/json'
        }

        request_data = json.dumps(content_version_payload).encode('utf-8')
        req = urllib.request.Request(upload_url, data=request_data, headers=headers, method='POST')

        try:
            with urllib.request.urlopen(req) as response:
                response_data = json.load(response)
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'File uploaded successfully to Salesforce.',
                        'SalesforceResponse': response_data
                    })
                }

        except urllib.error.HTTPError as e:
            error_response = e.read().decode('utf-8')
            return {
                'statusCode': e.code,
                'body': json.dumps({
                    'error': 'Failed to upload file to Salesforce',
                    'details': error_response
                })
            }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
