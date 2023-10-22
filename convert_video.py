import subprocess


def convert_video(input_path, output_path):
    return_code = subprocess.call(
        ["ffmpeg", "-i", input_path, "-loglevel", "quiet", output_path]
    )
    if return_code != 0:
        print(f"El proceso FFmpeg ha devuelto un código de error {return_code}.")
        
