import subprocess


def convert_video(input_path, output_path):
    subprocess.call(['ffmpeg', '-i', input_path, output_path])