from celery import Celery
from pathlib import Path
from pydub import AudioSegment
import requests
import smtplib
import os
from urllib3.exceptions import InsecureRequestWarning

app = Celery( 'tasks' , broker = 'redis://localhost:6379/0' )


@app.task(name='tasks.check')
def check():
    URL = "https://172.24.41.204/api/forms/pendingToConvert"
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
        cmd = "ffmpeg -y -i {} {}".format(pathOriginal, mp3_file)
        os.system(cmd)

        response = requests.put('https://172.24.41.204/api/form/' + str(id_form),
        json={"state": "Convertida", "formatted": mp3_file}, verify=False)
        print(response)
    
    
        with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()

            smtp.login('nicofrost87@gmail.com', 'PONERCONTRASEÑA')

            subject = "Confirmacion de publicacion de audio"
            body = "Saludos, le informamos que el audio con el que esta participando en el concurso fue publicado satisfactoriamente"

            msg = f'Subject: {subject}\n\n{body}'

            smtp.sendmail('rafaelroperolayton@gmail.com', email, msg)


app.conf.beat_schedule ={
    "run-me-every-te-seconds": {
        "task" : "tasks.check",
        "schedule" : 30.0
    }
}