from celery import Celery
from pathlib import Path
from pydub import AudioSegment
import requests
import smtplib
import os
import datetime
import pytz

app = Celery( 'tasks' , broker = 'redis://localhost:6379/0' )


@app.task(name='tasks.check')
def check():
    URL = "https://172.24.41.204/api/forms/pendingToConvert"
    r = requests.get(url = URL)
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
        startConv = datetime.strptime(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone("America/New_York")).strftime('%Y-%m-%dT%H:%M:%S'),'%Y-%m-%dT%H:%M:%S')
        cmd = "ffmpeg -y -i {} {}".format(pathOriginal, mp3_file)
        os.system(cmd)
        endConv = datetime.strptime(datetime.utcnow().replace(tzinfo=pytz.utc).astimezone(pytz.timezone("America/New_York")).strftime('%Y-%m-%dT%H:%M:%S'),'%Y-%m-%dT%H:%M:%S')

        response = requests.put('https://172.24.41.204/api/form/' + str(id_form),
        json={"state": "Convertida", "formatted": mp3_file, "startConvertion": startConv, "finishConversion": endConv})
        print(response)
    
    
        with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()

            smtp.login('cloudgrupo13@gmail.com', 'ormeefmgvsiwifzp')

            subject = "Confirmacion de publicacion de audio"
            body = "Saludos, le informamos que el audio con el que esta participando en el concurso fue publicado satisfactoriamente"

            msg = f'Subject: {subject}\n\n{body}'

            smtp.sendmail('cloudgrupo13@gmail.com', email, msg)


app.conf.beat_schedule ={
    "run-me-every-te-seconds": {
        "task" : "tasks.check",
        "schedule" : 30.0
    }
}