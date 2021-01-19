from datetime import datetime
import json
import logging
from pathlib import Path
from random import choices
import shutil
import sys
from typing import List, Tuple

import click
from nltk.tokenize import RegexpTokenizer
from os import path, walk


class NoCaptionError(Exception):
    """The media data contains no caption."""


def get_caption(raw_data: dict) -> str:
    try:
        caption_node = raw_data["edge_media_to_caption"]["edges"][0]["node"]
    except (KeyError, IndexError):
        raise NoCaptionError()

    return caption_node["text"]


def clean_token(raw_token: str):
    return raw_token.replace("@", "")

def clean_caption(raw_caption: str):
    tokens = (t for t in raw_caption.split() if t)
    return " ".join(clean_token(t) for t in tokens)

def image_data(image_id: int,
               image_filename: str,
               raw_data: dict,
               images_dirname: str,
               coco_id: int):
    
    assert raw_data["id"] == image_id
    
    raw_caption = get_caption(raw_data)
    
    caption = clean_caption(raw_caption)

    return f"<|startoftext|>{caption}<|endoftext|>"


def all_image_data(image_dir: str, media_dir: str, logger: logging.Logger):
    images_dirname = path.basename(path.normpath(image_dir))

    _, _, filenames = next(walk(image_dir))
    filenames_by_id = {
        path.splitext(filename)[0]: filename for filename in filenames
    }

    coco_id = 0

    for dirpath, _, filenames in walk(media_dir):

        if "data.json" not in filenames:
            continue

        image_id = path.basename(path.normpath(dirpath))

        if image_id not in filenames_by_id:
            continue

        with open(path.join(dirpath, "data.json"), 'r') as fd:
            raw_image_data =  json.load(fd)

        logger.info(f"Getting scraped data for image {image_id}...")

        if not raw_image_data:
            logger.warning(f"Failed to retrive scraped data for image {image_id}")
            continue

        image_filename = filenames_by_id[image_id]
        
        try: 
            caption = image_data(image_id,
                                 image_filename,
                                 raw_image_data,
                                 images_dirname,
                                 coco_id=coco_id)
        except NoCaptionError:
            logger.info(f"Image {image_id} has no caption. Skipping...")
            continue

        coco_id += 1
        logger.info(f"Generated GPT2 data from scraped data for image {image_id}")

        yield caption


@click.command()
@click.option(
    "--images-directory",
    "-i",
    "images_dir",
    type=click.Path(exists=True),
    required=True
)
@click.option(
    "--media-directory",
    "-m",
    "media_dir",
    type=click.Path(exists=True),
    required=True,
)
@click.option(
    "--output-directory", 
    "-o", 
    "output_dir", 
    type=click.Path(file_okay=False),
    required=True
)
@click.option('--dataset-name', '-n', type=str, required=True)
@click.option('--log-level', '-l', type=str, default='DEBUG')
def make_dataset(images_dir, 
                 media_dir,
                 output_dir,
                 dataset_name,
                 log_level):

    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)

    all_captions = list(all_image_data(images_dir, media_dir, logger))
    data_path = path.join(output_dir, f"captions.txt")

    with open(data_path, "w") as fileobj:
        fileobj.write("\n".join(all_captions))


if __name__ == "__main__":
    make_dataset()
