from celery import Celery
from pathlib import Path
from pydub import AudioSegment
import requests
import smtplib
import os
from datetime import datetime 
import pytz
from urllib3.exceptions import InsecureRequestWarning
import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content
import json
import boto3

redis_url = os.getenv('REDISTOGO_URL', 'redis://localhost:6379')
app = Celery( 'tasks' , broker = redis_url )

global conCorreo
conCorreo = False

global urlPath
urlPath = "https://backendgrupo13.herokuapp.com/"

global esS3
esS3 = True

global S3_BUCKET 
S3_BUCKET = "grupo13s3"

#PATH_GUARDAR_GLOBAL = '/home/ubuntu/BackendProyecto1/'
#PATH_GUARDAR_GLOBAL = 'D:/Nirobe/202120-Grupo07/CeleryProyecto1/'
PATH_GUARDAR_GLOBAL = '/app/'

path="uploads"
isExist = os.path.exists(path)
if not isExist:
    os.makedirs(path)

path="converted"
isExist = os.path.exists(path)
if not isExist:
    os.makedirs(path)

path="photos"
isExist = os.path.exists(path)
if not isExist:
    os.makedirs(path)

@app.task(name='tasks.check')
def check():

    if esS3 == True:
        URL = urlPath
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

        startConv = str(datetime.strptime(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone("America/New_York")).strftime('%Y-%m-%dT%H:%M:%S'),'%Y-%m-%dT%H:%M:%S'))

        receiveMessage = receiveMessageQueue()
        print(receiveMessage)
        jsonbody = ""
        ReceiptHandle = ""
        for message in receiveMessage:
            message_body = message["Body"]
            jsonbody = json.loads(message_body)
            ReceiptHandle = message['ReceiptHandle']

        print(jsonbody)
        print(ReceiptHandle)

        nombreArchivo = jsonbody['key']
        name =  nombreArchivo
        id_form = jsonbody['id']
        email = jsonbody['email']
        pathOriginal = os.path.join(PATH_GUARDAR_GLOBAL, name)    
        download_file(name, S3_BUCKET )

        input_file_name = os.path.basename(name).split('.')[0] 
        name_mp3_file = "converted/" + input_file_name + ".mp3"
        mp3_file = PATH_GUARDAR_GLOBAL + name_mp3_file
        cmd = "ffmpeg -y -i {} {}".format(pathOriginal, mp3_file)
        os.system(cmd)

        if conCorreo == True:
            subject = "Confirmacion de publicacion de audio"
            body = "Saludos, le informamos que el audio con el que esta participando en el concurso fue publicado satisfactoriamente"
            msg = f'Subject: {subject}\n\n{body}'
            sg = sendgrid.SendGridAPIClient(api_key='')
            from_email = Email('r.ropero@gmail.com')  # Change to your verified sender
            to_email = To(email)  # Change to your recipient
            mail = Mail(from_email, to_email, subject, msg)

        os.remove(pathOriginal)

        response = upload_file(name_mp3_file, S3_BUCKET, mp3_file)
        os.remove(mp3_file)

        endConv = str(datetime.strptime(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone("America/New_York")).strftime('%Y-%m-%dT%H:%M:%S'),'%Y-%m-%dT%H:%M:%S'))

        response = requests.put(urlPath + "api/form/" + str(id_form),
        json={"state": "Convertida", "formatted": name_mp3_file, "startConversion": startConv, "finishConversion": endConv}, verify=False)
        print(response)
        delete_message(ReceiptHandle)


    else:
        URL = urlPath + "api/forms/pendingToConvert"
        requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
        r = requests.get(url = URL, verify=False)
        form = r.json()
        print(form)
        #for form in data:
        id_form = form['id']
        print("idform " + str(id_form))
        email = form['email']
        print("email " +str(email))
        name = form['name']
        print("name " + str(name))
        lastname = form['lastname']
        pathOriginal = form['original']    
        print("pathOriginal " + str(pathOriginal))

        dir_name = os.path.dirname(pathOriginal)
        print("dir_name " + str(dir_name))
        input_file_name = os.path.basename(pathOriginal).split('.')[0] 
        print("input_file_name " + str(input_file_name))
        mp3_file = dir_name + "/" + input_file_name + ".mp3"
        print("mp3_file " + str(mp3_file))

        startConv = str(datetime.strptime(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone("America/New_York")).strftime('%Y-%m-%dT%H:%M:%S'),'%Y-%m-%dT%H:%M:%S'))
        cmd = "ffmpeg -y -i {} {}".format(pathOriginal, mp3_file)
        os.system(cmd)

        if conCorreo == True:
            subject = "Confirmacion de publicacion de audio"
            body = "Saludos, le informamos que el audio con el que esta participando en el concurso fue publicado satisfactoriamente"
            msg = f'Subject: {subject}\n\n{body}'
            sg = sendgrid.SendGridAPIClient(api_key='')
            from_email = Email('r.ropero@gmail.com')  # Change to your verified sender
            to_email = To(email)  # Change to your recipient
            mail = Mail(from_email, to_email, subject, msg)

            # Get a JSON-ready representation of the Mail object
            mail_json = mail.get()

            # Send an HTTP POST request to /mail/send
            mailresponse = sg.client.mail.send.post(request_body=mail_json)
            print(mailresponse.status_code)
            print(mailresponse.headers)
        
        endConv = str(datetime.strptime(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone("America/New_York")).strftime('%Y-%m-%dT%H:%M:%S'),'%Y-%m-%dT%H:%M:%S'))

        response = requests.put(urlPath + "api/form/" + str(id_form),
        json={"state": "Convertida", "formatted": mp3_file, "startConversion": startConv, "finishConversion": endConv}, verify=False)
        print(response)

def upload_file(file_name, bucket, filePath):
    """
    Function to upload a file to an S3 bucket
    """
    object_name = file_name
    s3_client = boto3.client('s3', 
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    aws_session_token=os.environ['AWS_SESSION_TOKEN'],)
    response = s3_client.upload_file(filePath, bucket, object_name)

    return response

def download_file(file_name, bucket):
    """
    Function to download a given file from an S3 bucket
    """
    pathdownload = os.path.join(PATH_GUARDAR_GLOBAL, file_name)

    s3 = boto3.client('s3', 
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    aws_session_token=os.environ['AWS_SESSION_TOKEN'],)
    s3.download_file(bucket, file_name, pathdownload)

    return pathdownload

def receiveMessageQueue():
    # Create SQS client
    sqs_client = boto3.client('sqs', region_name = 'us-east-1', 
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    aws_session_token=os.environ['AWS_SESSION_TOKEN'],)

    response = sqs_client.receive_message(
    QueueUrl="https://sqs.us-east-1.amazonaws.com/146202439559/MyQueue",
    MaxNumberOfMessages=1,
    WaitTimeSeconds=10,
    )

    print(f"Number of messages received: {len(response.get('Messages', []))}")

    return response.get("Messages", [])
    #for message in response.get("Messages", []):
    #    message_body = message["Body"]
    #    print(f"Message body: {json.loads(message_body)}")
    #    print(f"Receipt Handle: {message['ReceiptHandle']}")

    #return json.loads(message["Body"])

def delete_message(receipt_handle):
    sqs_client = boto3.client("sqs", region_name="us-east-1", 
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    aws_session_token=os.environ['AWS_SESSION_TOKEN'],)
    response = sqs_client.delete_message(
        QueueUrl="https://sqs.us-east-1.amazonaws.com/146202439559/MyQueue",
        ReceiptHandle=receipt_handle,
    )
    print(response)


app.conf.beat_schedule ={
    "run-me-every-te-seconds": {
        "task" : "tasks.check",
        "schedule" : 30.0
    }
}