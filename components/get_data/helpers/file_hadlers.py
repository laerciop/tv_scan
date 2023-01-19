"""Module to organize File Handling functions."""

import urllib.request
import os
from datetime import datetime
import pandas as pd
import moviepy.editor as mp


def audio_processor_helper(video_loc, aud_saving_folder):
    try:
        aud_file_name = video_loc.split(sep='/')[-1][:-4]+".wav"
        save_to = os.path.join(aud_saving_folder, aud_file_name)
        clip = mp.VideoFileClip(video_loc)
        clip.audio.write_audiofile(save_to, ffmpeg_params=["-ac", "1"], )
    except:
        now = datetime.now()
        record = f'Problems dealing with {aud_file_name} - {now}'
        with open('/log/log_audio_prep.txt', mode='a+') as writer:
            writer.write(record+'\n')


def spot_download_helper(url, saving_folder):
    """Functions to organize File Handling functions."""
    save_to = os.path.join(saving_folder, url.split(sep='/')[-1])
    try:
        urllib.request.urlretrieve(url, save_to)
    except:
        now = datetime.now()
        record = f'Problems dealing with {url} - {now}'
        with open('/log/log.txt', mode='a+') as writer:
            writer.write(record+'\n')

if __name__ == "__main__":
    pass
