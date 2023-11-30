
import base64
from datetime import datetime
import os
import signal
import sys
from types import FrameType

from flask import Flask, request
from sqlalchemy import create_engine
from convert_video import convert_video

from gcp.cloud_storage import BLOB_FORMAT, download_file_from_bucket, upload_to_bucket
from models import Status, Task, db, Formats

from utils.logging import logger

db_conn = os.environ.get(
    "DB_CONN", "postgresql://postgres:postgres@127.0.0.1:15432/oct"
)
upload_folder = os.environ.get("UPLOAD_FOLDER", "videos")

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = db_conn
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["PROPAGATE_EXCEPTIONS"] = True

app_context = app.app_context()
app_context.push()
engine = create_engine(app.config["SQLALCHEMY_DATABASE_URI"])
db.session.configure(bind=engine)
db.init_app(app)


@app.route("/")
def health() -> str:
    return "Worker Online Conversion Tool"

@app.route("/tasks", methods=["POST"])
def process_task() -> str:
    try:
        """Receive and parse Pub/Sub messages."""
        envelope = request.get_json()
        if not envelope:
            msg = "no Pub/Sub message received"
            print(f"error: {msg}")
            return f"Bad Request: {msg}", 400

        if not isinstance(envelope, dict) or "message" not in envelope:
            msg = "invalid Pub/Sub message format"
            print(f"error: {msg}")
            return f"Bad Request: {msg}", 400

        pubsub_message = envelope["message"]

        task_id = None
        if isinstance(pubsub_message, dict) and "data" in pubsub_message:
            task_id = int(base64.b64decode(pubsub_message["data"]).decode("utf-8").strip())

        if task_id is None:
            raise ValueError("El campo 'taskId' no está presente en la solicitud JSON.")
        process_video(task_id)
        return "ok"
    except Exception as e:
        logger.error(e)
        return e, 400

def process_video(task_id):
    try:
        task = db.session.query(Task).filter_by(id=task_id, status=Status.UPLOADED).first()
        if task is not None:
            task.inprocess = datetime.utcnow()
            task.status = Status.INPROCESS
            db.session.commit()

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
            os.makedirs(os.path.join("temp", str(task.user_id), "output"), exist_ok=True)
            download_file_from_bucket(input_blob_name, input_file_name)

            convert_video(input_file_name, output_file_name)

            upload_to_bucket(output_blob_name, output_file_name)

            if os.path.exists(input_file_name):
                os.remove(input_file_name)
            if os.path.exists(output_file_name):
                os.remove(output_file_name)
            task.processed = datetime.utcnow()
            task.status = Status.PROCESSED
            db.session.commit()
    except Exception as ex:
        raise(ex)

def shutdown_handler(signal_int: int, frame: FrameType) -> None:
    logger.info(f"Caught Signal {signal.strsignal(signal_int)}")

    from utils.logging import flush

    flush()

    # Safely exit program
    sys.exit(0)


if __name__ == "__main__":
    # Running application locally, outside of a Google Cloud Environment

    # handles Ctrl-C termination
    signal.signal(signal.SIGINT, shutdown_handler)

    app.run(host="localhost", port=8080, debug=True)
else:
    # handles Cloud Run container termination
    signal.signal(signal.SIGTERM, shutdown_handler)