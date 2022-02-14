from celery import Celery
import requests

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
        pathOriginal = form['original']

        

        print (id_form)

app.conf.beat_schedule ={
    "run-me-every-te-seconds": {
        "task" : "tasks.check",
        "schedule" : 10.0
    }
}