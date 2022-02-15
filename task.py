from celery import Celery
from pathlib import Path
from pydub import AudioSegment
import requests
import os
import ffmpeg

app = Celery( 'tasks' , broker = 'redis://localhost:6379/0' )

@app.task(name='tasks.check')
def check():
    URL = "http://127.0.0.1:5000/api/forms/"
    r = requests.get(url = URL)
    data = r.json()

    for form in data:
        id_form = form['id']
        email = form['email']
        name = form['name']
        lastname = form['lastname']
        pathOriginal = form['original'].replace("/", "\\")   
    
        dir_name = os.path.dirname(pathOriginal)
        input_file_name = os.path.basename(pathOriginal).split('.')[0] 
        mp3_file = dir_name + "\\" + input_file_name + ".mp3"
        cmd = "ffmpeg -y -i {} {}".format(pathOriginal, mp3_file)
        os.system(cmd)

        print(mp3_file)
        print(mp3_file.replace("\\", "/"))
        print('http://127.0.0.1:5000/api/form/' + str(id_form))
        response = requests.put('http://127.0.0.1:5000/api/form/' + str(id_form),
        json={"state": "Convertida", "formatted": mp3_file.replace("\\", "/") })
        print(response)

app.conf.beat_schedule ={
    "run-me-every-te-seconds": {
        "task" : "tasks.check",
        "schedule" : 30.0
    }
}