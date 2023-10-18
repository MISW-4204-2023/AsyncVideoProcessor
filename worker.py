from datetime import datetime
import os
from celery import Celery
from convert_video import convert_video
from models import Status, Task, session

broker = os.environ.get("REDIS_CONN", "redis://localhost:6379/0")
celery = Celery("tasks", broker=broker)


@celery.task(name="process_video")
def process_video(task_id):
    task = session.query(Task).filter_by(id=task_id, status=Status.UPLOADED).first()
    if task is not None:
        os.makedirs(os.path.join("videos", str(task.user_id), "output"), exist_ok=True)
        convert_video(
            "videos/{}/input/{}.{}".format(task.user_id, task.id, task.input_format.value),
            "videos/{}/output/{}.{}".format(task.user_id, task.id, task.output_format.value),
        )
        task.processed = datetime.utcnow()
        task.status = Status.PROCESSED
        session.commit()
