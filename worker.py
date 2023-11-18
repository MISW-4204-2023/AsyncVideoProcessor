from datetime import datetime
import os
from convert_video import convert_video
from gcp.cloud_storage import BLOB_FORMAT, download_file_from_bucket, upload_to_bucket
from google.cloud import pubsub_v1
from models import Status, Task, engine
from sqlalchemy.orm import sessionmaker


upload_folder = os.environ.get("UPLOAD_FOLDER", "videos")


def subscriber_gcp():
    with pubsub_v1.SubscriberClient() as subscriber:
        subscription_path = (
            "projects/cloud-uniandes-403120/subscriptions/testSuscription"
        )
        flow_control = pubsub_v1.types.FlowControl(max_messages=4)

        streaming_pull_future = subscriber.subscribe(
            subscription_path, callback=process_video, flow_control=flow_control
        )
        streaming_pull_future.result()


def process_video(message: pubsub_v1.subscriber.message.Message):
    try:
        print("Procesando el archivo {}".format(message.data))
        task_id = int(message.data)
        Session = sessionmaker(bind=engine)
        session = Session()
        task = session.query(Task).filter_by(id=task_id, status=Status.UPLOADED).first()
        if task is not None:
            task.inprocess = datetime.utcnow()
            task.status = Status.INPROCESS
            session.commit()

            file_name_input = "{}.{}".format(task.id, task.input_format.value)
            file_name_output = "{}.{}".format(task.id, task.output_format.value)
            input_file_name = os.path.join(
                "temp", str(task.user_id), "input", file_name_input
            )
            output_file_name = os.path.join(
                "temp", str(task.user_id), "output", file_name_output
            )

            input_blob_name = BLOB_FORMAT.format(
                upload_folder,
                str(task.user_id),
                "input",
                task.id,
                task.input_format.value,
            )
            output_blob_name = BLOB_FORMAT.format(
                upload_folder,
                str(task.user_id),
                "output",
                task.id,
                task.output_format.value,
            )

            os.makedirs(os.path.join("temp", str(task.user_id), "input"), exist_ok=True)
            download_file_from_bucket(input_blob_name, input_file_name)

            convert_video(input_file_name, output_file_name)

            os.makedirs(
                os.path.join("temp", str(task.user_id), "output"), exist_ok=True
            )
            upload_to_bucket(output_blob_name, output_file_name)

            if os.path.exists(input_file_name):
                os.remove(input_file_name)
            if os.path.exists(output_file_name):
                os.remove(output_file_name)
            task.processed = datetime.utcnow()
            task.status = Status.PROCESSED
            session.commit()
            message.ack()
    except Exception as ex:
        print(ex.message)
        print("Error procesando el archivo {}".format(message.data))
        message.nack()


subscriber_gcp()
