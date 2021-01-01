"""Prepares MSCOCO-like dataset for the im2txt model"""

from datetime import datetime
import json
import logging
from pathlib import Path
from random import choices
import re
import shutil
import sys
from typing import Iterable, List, Tuple

import click
from os import path, walk

COCO_SPLIT_PROPORTIONS = {'val': 0.3, 'train': 0.7}


def split_choices():
    population = list(COCO_SPLIT_PROPORTIONS.keys())
    weights = list(COCO_SPLIT_PROPORTIONS.values())

    while True:
        yield choices(population, weights=weights)[0]

split_choice = split_choices()


class NoCaptionError(Exception):
    """The media data contains no caption."""


def get_caption(raw_data: dict) -> str:
    try:
        caption_node = raw_data["edge_media_to_caption"]["edges"][0]["node"]
    except (KeyError, IndexError):
        raise NoCaptionError()

    return caption_node["text"]


def tokenize(caption: str) -> List[str]:
    """
    Reconstuct caption with no punctuation except for "#", "`"and "'"
    Usernames are split on "." and "_", and do not start with "@"
    """
    pattern = "[A-Za-z0-9'\`#@%,.-_?]+(?:\`[A-Za-z]+)?"
    handle_pattern = "[A-Za-z0-9]+(?:\`[A-Za-z]+)?"
    punctuation = (",", ".", "?", ":", ";", "!")

    for token in re.findall(pattern, caption):
        if len(token) == 1 and token != "a":
            continue

        if "@" in token:
            # Also addresses case where '@' somehow ends up in middle of token
            yield from re.findall(handle_pattern, token)
        elif any(token.endswith(punctuation) for punctuation in punctuation):
            yield token[:-1]
        else:
            yield token

def clean_caption_and_tokens(raw_caption: str) -> Tuple[str, List[str]]:
    tokens = list(tokenize(raw_caption))
    print(f"Tokens: {tokens}")
    caption = " ".join(tokens)
    return caption, tokens


def image_data(image_id: int,
               image_filename: str,
               raw_data: dict,
               images_dirname: str,
               coco_id: int):

    assert int(raw_data["id"]) == image_id

    split = next(split_choice)
    output_dirname = f"{images_dirname}_{split}"

    raw_caption = get_caption(raw_data)
    caption, tokens = clean_caption_and_tokens(raw_caption)

    sentence = {
        "raw": caption,
        "tokens": tokens,
        "imid": image_id,
        "sentid": image_id,
    }

    return {
        "filepath": output_dirname,
        "sentids": [image_id],
        "filename": image_filename,
        "imgid": image_id,
        "split": split,
        "sentences": [sentence],
        "cocoid": coco_id,
    }


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
            coco_data = image_data(int(image_id),
                                   image_filename,
                                   raw_image_data,
                                   images_dirname,
                                   coco_id=coco_id)
        except NoCaptionError:
            logger.info(f"Image {image_id} has no caption. Skipping...")
            continue

        coco_id += 1
        logger.info(f"Generated COCO data from scraped data for image {image_id}")

        yield coco_data


def copy_image_to_dataset(data: dict,
                          from_dir: str,
                          to_dir: str,
                          logger: logging.Logger):

    directory = path.join(to_dir, data["filepath"])
    # Create directory if absent
    Path(directory).mkdir(parents=True, exist_ok=True)

    filename = data["filename"]
    from_path = path.join(from_dir, filename)
    to_path = path.join(directory, filename)

    logger.info(f"Copying {from_path} to {to_path}")
    shutil.copyfile(from_path, to_path)


def belongs_to_split(split_name: str):
    def predicate(datum: dict):
        return datum["split"] == split_name
    return predicate

def im2txt_coco_data(image_data: Iterable[dict], info: dict) -> dict:
    images = []
    annotations = []
    for datum in image_data:
        images.append(dict(file_name=datum["filename"], id=datum["imgid"]))
        annotations.append(dict(id=datum["cocoid"],
                                image_id=datum["imgid"],
                                caption=datum["sentences"][0]["raw"]))

    return {
        "info": info,
        "images": images,
        "annotations": annotations,
    }


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
def make_coco_dataset(images_dir,
                      media_dir,
                      output_dir,
                      dataset_name,
                      log_level):

    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)

    all_images_data = list(all_image_data(images_dir, media_dir, logger))
    info = {"dataset": dataset_name, "date_created": datetime.now().isoformat()}

    for split_name in ('train', 'val'):
        split_data = filter(belongs_to_split(split_name), all_images_data)
        coco_data_path = path.join(output_dir, f"captions-{split_name}.json")
        with open(coco_data_path, "wb") as fileobj:
            data = im2txt_coco_data(split_data, {**info, "split": split_name})
            fileobj.write(json.dumps(data).encode('ascii'))

    for image_datum in all_images_data:
        copy_image_to_dataset(image_datum, images_dir, output_dir, logger)


if __name__ == "__main__":
    make_coco_dataset()

