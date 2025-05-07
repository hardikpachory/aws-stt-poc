import uuid
import json
import os
import boto3
import urllib.request
import urllib.error
import time
from datetime import datetime, timedelta
from botocore.exceptions import ClientError

transcribe = boto3.client('transcribe')
s3 = boto3.client('s3')
comprehend = boto3.client('comprehend')
conversation_response = ''
field_visit_json = ''
whatId_from_file=''
ownerId_from_file=''
def lambda_handler(event, context):
    global whatId_from_file
    global ownerId_from_file
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    source_filename = source_key.split('.')[0]
    whatId_from_file = source_filename.split('-')[1]
    ownerId_from_file = source_filename.split('-')[2]
    output_bucket = 'aws-poc-output-metacube'
    transcribe_bucket = 'aws-poc-transcribe-metacube'
    extension = os.path.splitext(source_key)[1][1:]
    
    job_name = f"transcription-{uuid.uuid4()}"
    
    print(f"Transcription job {source_bucket} {source_key} {job_name} started.")

    transcription_url = transcribe_audio_to_text(source_bucket, source_key, job_name, transcribe_bucket, extension)
    transcribed_text = wait_for_transcription(job_name)

    

    if transcribed_text:
        # summarized_text = summarize_text_using_comprehend(transcribed_text)
        # key_phrases_text = '\n'.join([key_phrase['Text'] for key_phrase in summarized_text])
        # print(F"key_phrases_text: {key_phrases_text}")
        
        # Conversation
        print("Transcription Complete!")
        conversation_file_key = f"conversation-{source_filename}.txt"
        conversation_text = convert_transcript_to_conversation(conversation_response)
        save_conversation_to_s3(conversation_text, conversation_file_key, output_bucket)
        print(f"Conversation saved!")
        # Not Saving AI Summary
        # ai_job_name = f"ai-job-{uuid.uuid4()}"
        # ai_summary_file_key = f"aisummary-{source_filename}.txt"
        # ai_summary = convert_conversation_to_aisummary(conversation_text)
        # save_aisummary_to_s3(ai_summary, ai_summary_file_key, output_bucket)

        # Field Visit
        field_visit_json = convert_transcript_to_JSON(conversation_text)
        print(f"field_visit_json: {field_visit_json}")
        print(f" Type of the field Visit JSON: {type(field_visit_json)}")
        event_created = create_event(field_visit_json)
        result_key = f"field-visit-{source_filename}.json"
        save_field_visit_to_s3(field_visit_json, result_key, output_bucket)

        # Not Saving NLP Summary
        # result_key = f"summarized-{source_filename}.txt"
        # save_summarized_text_to_s3(key_phrases_text, result_key, output_bucket)
        # create_event(field_visit_json)
        return {
            'statusCode': 200,
            'body': json.dumps(f"Summary saved to {result_key} in {output_bucket}.")
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps("Error in transcription or summarization.")
        }


def convert_transcript_to_JSON(transcripted_text):
    bedrock = boto3.client(service_name='bedrock-runtime', region_name='us-east-1')
    todayDateTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    afterOneHour = (datetime.now() + timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    day = datetime.now().day
    thisMonth = datetime.now().month
    thisYear = datetime.now().year
    dayOfWeek = datetime.now().strftime('%A')
    # transcripted_text = f'Extract the following details from the given sales visit summary and return the result in JSON format with the exact keys as specified: subject (what kind of event among - "Field Visit", "Scheduled Call", "Appointment", "Set Up", "Other", "Follow Up", "Demo", "Introduction", "Suppport", "Proposal"), Location (Location at where the event happened.),event_status (either "Complete" or "Incomplete / Canceled"), activity_type (among following - "Drop-In", "Meeting", "Demo", "Implementation", "Onboarding", "Negotiation" ), contact_person (The name of the person met during the visit), startDateTime (The date and time when this event started in "YYYY-MM-DD HH:MM:SS" Format. Today is {todayDateTime}. Calculate the startDateTime based on today. If any one of the day,month or year is missing, extract them from {todayDateTime}. Entire default value is {todayDateTime}.), isDateTime (value is "true" if time in "AM" or "PM" is mentioned. Otherwise the value is "false"), endDateTime (The date and time when event ended in "YYYY-MM-DD HH:MM:SS" Format. Today is {todayDateTime}. Calculate the endtDateTime based on today. If any one of the day,month or year is missing, extract them from {todayDateTime}. Entire default value is {afterOneHour}.), IsAllDayEvent(value is "true" if it is an all day event otherwise "false"), PreScheduled (value is "true" if it is a pre scheduled event otherwise "false"), discussion_summary (A proper and in detail summary of the discussion during the visit, challenges and outcome. Mention in detail about the next steps to be taken with description and date.), meeting_outcome (one of the following - "Staff Conversation", "DM Conversation", "Stalled", "Not Interested" ) ,Next_step (value must one of the given options - "Email", "Call", "Meeting", "Drop In", "Demo" ), Next_step_desc (One line description of the next steps), next_step_date (Today is {todayDateTime}. Based on today, find out the date on which next step is to be executed in "YYYY-MM-DD HH:MM:SS" Format. Default is null). Only return the JSON Object Don''t need any explaination or code reference.Below is the summmary - ' + transcripted_text
    #optimized_prompt = f"Extract structured details from the given sales visit summary and return only a JSON object. Fields: subject (One of ['Field Visit', 'Scheduled Call', 'Appointment', 'Set Up', 'Other', 'Follow Up', 'Demo', 'Introduction', 'Support', 'Proposal']), Location (Event location), event_status ('Complete' or 'Incomplete / Canceled'), activity_type (One of ['Drop-In', 'Meeting', 'Demo', 'Implementation', 'Onboarding', 'Negotiation']), contact_person (Name of the person met), startDateTime ('YYYY-MM-DD HH:MM:SS', default {todayDateTime} if missing), isDateTime ('true' if AM/PM mentioned, else 'false'), endDateTime ('YYYY-MM-DD HH:MM:SS', default {afterOneHour} if missing), IsAllDayEvent ('true' or 'false'), PreScheduled ('true' or 'false'), discussion_summary (Key points, challenges, outcomes, next steps with dates), meeting_outcome (One of ['Staff Conversation', 'DM Conversation', 'Stalled', 'Not Interested']), Next_step (One of ['Email', 'Call', 'Meeting', 'Drop In', 'Demo']), Next_step_desc (Brief next step description), next_step_date ('YYYY-MM-DD HH:MM:SS' or null if not provided). Example: Summary: 'Visited ABC office for a demo session. Met John Doe at 3 PM today. Demo completed successfully. Discussed product features and pricing concerns. Follow-up meeting scheduled for 2025-03-25 at 4 PM.' Output: {{'subject': 'Demo', 'Location': 'ABC Office', 'event_status': 'Complete', 'activity_type': 'Demo', 'contact_person': 'John Doe', 'startDateTime': '2025-03-19 15:00:00', 'isDateTime': 'true', 'endDateTime': '2025-03-19 16:00:00', 'IsAllDayEvent': 'false', 'PreScheduled': 'true', 'discussion_summary': 'Demo completed, pricing concerns discussed. Follow-up on 2025-03-25 at 16:00:00.', 'meeting_outcome': 'DM Conversation', 'Next_step': 'Meeting', 'Next_step_desc': 'Pricing discussion', 'next_step_date': '2025-03-25 16:00:00'}} Now extract details from: {transcripted_text} and return only the JSON object."
    optimized_prompt = f"""
    Extract the following details from the given sales visit summary and return the result in JSON format with the exact keys as specified:

    - **subject**: One of ["Field Visit", "Scheduled Call", "Appointment", "Set Up", "Other", "Follow Up", "Demo", "Introduction", "Support", "Proposal"].  
    - **Location**: The location where the event happened.  
    - **event_status**: Either "Complete" or "Incomplete / Canceled".  
    - **activity_type**: One of ["Drop-In", "Meeting", "Demo", "Implementation", "Onboarding", "Negotiation"].  
    - **startDateTime**: The date and time when this event started in "YYYY-MM-DD HH:MM:SS" format.  
        - Today is {todayDateTime}, and the day is {dayOfWeek}.  
        - Convert relative time references (e.g., "tomorrow at 3 PM", "next Friday","on Monday") into absolute datetime using today's date.  
        - Assume 00:00:00 if only a date is given.  
        - Infer missing dates from context and handle multiple formats.  
        - Default value is {todayDateTime}.  
    - **endDateTime**: The date and time when this event ended in "YYYY-MM-DD HH:MM:SS" format.
        - Today is {todayDateTime}, and the day is {dayOfWeek}.  
        - Convert relative time references (e.g., "tomorrow at 3 PM", "next Friday","on Monday") into absolute datetime using today's date.  
        - Assume 00:00:00 if only a date is given.  
        - Infer missing dates from context and handle multiple formats.  
        - Default value is {afterOneHour}.  
    - **IsAllDayEvent**: "true" if it is an all-day event, otherwise "false".  
    - **PreScheduled**: "true" if it is a pre-scheduled event, otherwise "false".  
    - **discussion_summary**: A detailed summary of the discussion, including key points, challenges, and outcomes.  
        - Include next steps with descriptions and dates.  
    - **meeting_outcome**:  
        - If **event_status** is "Complete", choose from ["Staff Conversation", "DM Conversation", "Stalled", "Not Interested"].  
        - If **event_status** is "Incomplete / Canceled", choose from ["Business is Closed", "Not Available"].  
    - **Next_step**: One of the following values only - ["Email", "Call", "Meeting", "Drop In", "Demo"].  
    - **Next_step_desc**: A one-line description of the next step.  
    - **next_step_date**: The date on which the next step is to be executed in "YYYY-MM-DD HH:MM:SS" format.  
        - Convert relative time references (e.g., "tomorrow at 3 PM", "next Friday") into absolute datetime using today's date.  
        - Assume 00:00:00 if only a date is given.  
        - Infer missing dates from context and handle multiple formats.  
        - Default is null if not provided.  

    Return only the JSON object. Do not include any explanations, extra text, or code references.  

    **Below is the sales visit summary:**  
    '{transcripted_text}'
    """
    transcripted_text = optimized_prompt
    print(f"Required Prompt: {transcripted_text}")
    model_id = 'meta.llama3-8b-instruct-v1:0'
    try:
        response = bedrock.invoke_model(
            modelId=model_id,
            body=json.dumps({'prompt':transcripted_text, "max_gen_len": 4096, "temperature": 0.5, "top_p": 0.9}),
            contentType='application/json'
        )
        summary = json.loads(response['body'].read().decode('utf-8'))
        print(f"AI Returns this Body {summary}")
        summary = summary['generation']
        json_object = summary.split('{')
        json_object = '{' + json_object[1]
        json_object = json_object.split('}')
        json_object = json_object[0] + '}'
        print(f"Required JSON Object {json_object}")
        return json_object
    except ClientError as e:
        # Handle AWS error
        print(f"Error invoking model: {e}")
        json_object = "{}"
        return json_object

def transcribe_audio_to_text(bucket, key, job_name, transcribe_bucket, extension):
    print(f"Starting transcription job {bucket} {key} {job_name} {transcribe_bucket} {extension} started.")
    try:
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': f"s3://{bucket}/{key}"},
            MediaFormat=extension,
            LanguageCode='en-US',
            OutputBucketName= transcribe_bucket,
            Settings={
            'ShowSpeakerLabels': True,
            'MaxSpeakerLabels': 10,
            'ShowAlternatives': True,
            'MaxAlternatives': 2,
            }
        )
        return f"s3://{bucket}/{key}"
    except ClientError as e:
        print(f"Error starting transcription job: {e}")
        raise

def convert_transcript_to_conversation(conversation_json):
    print(conversation_json)
    convo = ''
    speaker = conversation_json[0]['speaker_label']
    convo = convo + speaker+" : "
    for i in conversation_json:
        if i['speaker_label'] == speaker:
            convo = convo + " " + i['transcript']
        else:
            speaker = i['speaker_label']
            convo = convo + "\n" + speaker + " : " + i['transcript']
    return convo

def convert_conversation_to_aisummary(conversation_transcript):
    bedrock = boto3.client(service_name='bedrock-runtime', region_name='us-east-1')
    conversation_transcript = 'You are an expert summarizer. Given the following conversation, generate a concise, high-level summary that captures the key topics, themes, and main points discussed. Focus on the overall purpose of the conversation rather than summarizing each individual sentence. Ensure that the summary provides a clear understanding of what the discussion was about without excessive details. Keep it objective, coherent, and to the point. Before the final summary, add "Summary:" in the response. \n' + conversation_transcript
    model_id = 'meta.llama3-8b-instruct-v1:0'
    try:
        response = bedrock.invoke_model(
            modelId=model_id,
            body=json.dumps({'prompt':conversation_transcript, "max_gen_len": 4096, "temperature": 0.5, "top_p": 0.9}),
            contentType='application/json'
        )
        summary = json.loads(response['body'].read().decode('utf-8'))
        print(f"Summary recieved {summary}")
        summary = summary['generation']
        splitted = summary.split('Summary:')
        summary = splitted[1]
        summary = summary.replace('```python', '')
        summary = summary.replace('```', '')
        print(f"Final Summary {summary}")
        return summary
    except ClientError as e:
        # Handle AWS error
        print(f"Error invoking model: {e}")
        summary = 'Error'
        return summary

def wait_for_transcription(job_name):
    global conversation_response
    print(f"Getting text job {job_name} started.")
    while True:
        result = transcribe.get_transcription_job(TranscriptionJobName=job_name)
        status = result['TranscriptionJob']['TranscriptionJobStatus']
        print(f"Transcription job {job_name} status: {status}")
        if status == 'COMPLETED':
            transcript_file_uri = result['TranscriptionJob']['Transcript']['TranscriptFileUri']
            print(f"Transcript file URI: {transcript_file_uri}")
            transcript_file_uri = result['TranscriptionJob']['Transcript']['TranscriptFileUri']
            url_without_protocol = transcript_file_uri.replace("https://", "")
            url_parts = url_without_protocol.split('/')
            bucket = url_parts[1]
            key = url_parts[2] 
            print(f"{bucket} : {key}") 
            response = s3.get_object(Bucket=bucket, Key=key)
            transcript = json.loads(response['Body'].read().decode('utf-8'))
            
            conversation_response = transcript['results']['audio_segments']
            return transcript['results']['transcripts'][0]['transcript']
        elif status == 'FAILED':
            print(f"Transcription failed for job {job_name}")
            return None
        time.sleep(5)


def summarize_text_using_comprehend(text):
    # Split the text into manageable chunks
    chunks = split_text_into_chunks(text)
    try:
        return process_chunks(chunks)
    except ClientError as e:
        print(f"Error summarizing text: {e}")
        raise


def save_summarized_text_to_s3(text, key, bucket):
    try:
        s3.put_object(Body=text, Bucket=bucket, Key=key, ContentType='text/plain')
    except ClientError as e:
        print(f"Error saving summarized text to S3: {e}")
        raise

def save_field_visit_to_s3(text, key, bucket):
    try:
        s3.put_object(Body=text, Bucket=bucket, Key=key, ContentType='text/plain')
    except ClientError as e:
        print(f"Error saving Field Visit text to S3: {e}")
        raise

def save_aisummary_to_s3(text, key, bucket):
    try:
        s3.put_object(Body=text, Bucket=bucket, Key=key, ContentType='text/plain')
    except ClientError as e:
        print(f"Error saving summarized text to S3: {e}")
        raise

def save_conversation_to_s3(text, key, bucket):
    try:
        s3.put_object(Body=text, Bucket=bucket, Key=key, ContentType='text/plain')
    except ClientError as e:
        print(f"Error saving summarized text to S3: {e}")
        raise

def split_text_into_chunks(text, max_size=5000):
    chunks = []
    while len(text) > max_size:
        # Find the last space to avoid cutting words in half
        split_point = text.rfind(' ', 0, max_size)
        if split_point == -1:
            # If no space is found, split at max_size
            split_point = max_size
        chunks.append(text[:split_point])
        text = text[split_point:].lstrip() 
    if text:
        chunks.append(text)  # Add the last remaining part
    return chunks


def process_chunks(chunks):
    all_key_phrases = []
    for chunk in chunks:
        response = comprehend.batch_detect_key_phrases(
            TextList=[chunk],
            LanguageCode='en'
        )
        for result in response['ResultList']:
            all_key_phrases.extend(result['KeyPhrases'])
    return all_key_phrases

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

def create_event(event_json):
    print("Creating Salesforce Event")
    print(f"Event JSON Parameter Type: {type(event_json)}")
    event_json = json.loads(event_json)
    StartDateTime = event_json.get("startDateTime")
    EndDateTime = event_json.get("endDateTime")
    
    if(event_json.get('IsAllDayEvent')=="true"):
        print("Updating AllDayEvent StartDate and EndDate")
        StartDateTime = StartDateTime.split()[0]+" 00:00:00"
        EndDateTime = EndDateTime.split()[0]+" 00:00:00"
    
    field_visit_payload = {
        "Subject": event_json.get('subject'),
        "Location": event_json.get('Location'), #New Data
        "IsAllDayEvent": event_json.get('IsAllDayEvent'), #New Data
        "PreScheduled": event_json.get('PreScheduled'), #New Data
        "Event_Status": event_json.get('event_status'),
        "Activity_type":event_json.get('activity_type'),
        "Meeting_Outcome":event_json.get('meeting_outcome'),
        "StartDateTime": StartDateTime,
        "EndDateTime": EndDateTime,
        "Description":event_json.get('discussion_summary'),
        "Next_Step":event_json.get('Next_step'),
        "Next_Step_Description":event_json.get('Next_step_desc'),
        "Next_Step_Date":event_json.get('next_step_date'),
        "WhoId": whatId_from_file,
        "OwnerId": ownerId_from_file
    }
    #WHATID FOR DEVELOER ACCOUNT FOR SQ ITS WHATID
    print(f"Field Visit Payload: {field_visit_payload}")
    sf_access_token, instance_url = get_sf_access_token()
    upload_url = f"{instance_url}/services/data/v59.0/sobjects/Event/"
    event_api_url = f'{instance_url}/services/apexrest/eventService/'
    headers = {
        "Authorization": f"Bearer {sf_access_token}",
        "Content-Type": "application/json"
    }
    print("Creating Request Data")
    request_data = json.dumps(field_visit_payload).encode('utf-8')
    print("Request Data Created")
    req = urllib.request.Request(event_api_url, data=request_data, headers=headers, method='POST')
    print("Request Created")
    try:
        print("Sending Request")
        with urllib.request.urlopen(req) as response:
            print(f"Response: {response}")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Request Sent successfully to Salesforce.'
                })
            }

    except urllib.error.HTTPError as e:
        error_response = e.read().decode('utf-8')
        print(f"Error in http call: {error_response}")
        return {
            'statusCode': e.code,
            'body': json.dumps({
                'error': 'Failed to Send Request to Salesforce',
                'details': error_response
            })
        }
