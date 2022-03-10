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

app = Celery( 'tasks' , broker = 'redis://localhost:6379/0' )


@app.task(name='tasks.check')
def check():
    URL = "http://44.202.144.123/api/forms/pendingToConvert"
    #URL = "http://127.0.0.1:5000/api/forms/pendingToConvert"
    
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

    r = requests.get(url = URL, verify=False)
    data = r.json()

    for form in data:
        id_form = form['id']
        email = form['email']
        name = form['name']
        lastname = form['lastname']
        pathOriginal = form['original']    
    
        dir_name = os.path.dirname(pathOriginal)
        input_file_name = os.path.basename(pathOriginal).split('.')[0] 
        mp3_file = dir_name + "/" + input_file_name + ".mp3"
        startConv = str(datetime.strptime(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone("America/New_York")).strftime('%Y-%m-%dT%H:%M:%S'),'%Y-%m-%dT%H:%M:%S'))
        cmd = "ffmpeg -y -i {} {}".format(pathOriginal, mp3_file)
        os.system(cmd)

        subject = "Confirmacion de publicacion de audio"
        body = "Saludos, le informamos que el audio con el que esta participando en el concurso fue publicado satisfactoriamente"
        msg = f'Subject: {subject}\n\n{body}'
        sg = sendgrid.SendGridAPIClient(api_key='')
        from_email = Email('cloudgrupo13@gmail.com')  # Change to your verified sender
        to_email = To(email)  # Change to your recipient
        mail = Mail(from_email, to_email, subject, msg)

        # Get a JSON-ready representation of the Mail object
        mail_json = mail.get()

        # Send an HTTP POST request to /mail/send
        mailresponse = sg.client.mail.send.post(request_body=mail_json)
        print(mailresponse.status_code)
        print(mailresponse.headers)
        endConv = str(datetime.strptime(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone("America/New_York")).strftime('%Y-%m-%dT%H:%M:%S'),'%Y-%m-%dT%H:%M:%S'))

        response = requests.put('http://44.202.144.123/api/form/' + str(id_form),
        #response = requests.put('http://127.0.0.1:5000/api/form/' + str(id_form),
        json={"state": "Convertida", "formatted": mp3_file, "startConversion": startConv, "finishConversion": endConv}, verify=False)
        print(response)

app.conf.beat_schedule ={
    "run-me-every-te-seconds": {
        "task" : "tasks.check",
        "schedule" : 30.0
    }
}