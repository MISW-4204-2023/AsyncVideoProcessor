from datetime import datetime
import os
from celery import Celery
from convert_video import convert_video
from gcp.cloud_storage import BLOB_FORMAT, download_file_from_bucket, upload_to_bucket
from models import Status, Task, session

broker = os.environ.get("REDIS_CONN", "redis://localhost:6379/0")
celery = Celery("tasks", broker=broker)
upload_folder = os.environ.get("UPLOAD_FOLDER", "videos")


@celery.task(name="process_video")
def process_video(task_id):
    task = session.query(Task).filter_by(id=task_id, status=Status.UPLOADED).first()
    if task is not None:
        task.inprocess = datetime.utcnow()
        task.status = Status.INPROCESS
        session.commit()

        file_name_input = "{}.{}".format(task.id, task.input_format.value)
        file_name_output = "{}.{}".format(task.id, task.output_format.value)
        input_file_name = os.path.join(
            "videos", str(task.user_id), "input", file_name_input
        )
        output_file_name = os.path.join(
            "videos", str(task.user_id), "output", file_name_output
        )

        input_blob_name = BLOB_FORMAT.format(
            upload_folder, str(task.user_id), "input", task.id, task.input_format.value
        )
        output_blob_name = BLOB_FORMAT.format(
            upload_folder,
            str(task.user_id),
            "output",
            task.id,
            task.output_format.value,
        )

        os.makedirs(os.path.join("videos", str(task.user_id), "input"), exist_ok=True)
        download_file_from_bucket(input_blob_name, input_file_name)

        convert_video(input_file_name, output_file_name)

        os.makedirs(os.path.join("videos", str(task.user_id), "output"), exist_ok=True)
        upload_to_bucket(output_blob_name, output_file_name)

        os.remove(input_file_name)
        os.remove(output_file_name)
        task.processed = datetime.utcnow()
        task.status = Status.PROCESSED
        session.commit()
