from datetime import datetime
import json
import logging
from random import choices
import shutil
from typing import List, Tuple

import click
from nltk.tokenize import RegexpTokenizer
from os import path, walk


COCO_SPLIT_PROPORTIONS = {
    'test': 0.040555776359226844,
    'restval': 0.24742268041237114,
    'val': 0.040555776359226844,
    'train': 0.6714657668691751
}


def split_choices():
    population = list(COCO_SPLIT_PROPORTIONS.keys())
    weights = list(COTO_SPLIT_PROPORTIONS.values())
    
    while True:
        yield choices(population, weights=weights)[0]

split_choice = split_choices()
insta_tokenizer = RegexpTokenizer(r"[#'_\w]+")


def load_raw_image_data(root_dir, image_id):
    for dirpath, _, filenames in walk(root_dir):
        
        if "data.json" not in filenames:
            continue
        
        dir_image_id = path.basename(path.normpath(dirpath))
        if dir_image_id == image_id:

            with open(path.join(dirpath, "data.json"), 'r') as fd:
                return json.load(fd)


def get_caption(raw_data: dict) -> str:
    caption_node = raw_data["edge_media_to_caption"]["edges"][0]
    return caption_node["text"]


def clean_caption_and_tokens(raw_caption: str) -> Tuple[str, List[str]]:
    tokens = insta_tokenizer.tokenize(raw_caption)
    # Reconstuct caption with no punctuation except for "#" and "'"
    # Usernames are split on "." and "_", and do not start with "@"
    caption = " ".join(tokens)
    return caption, cleaned_tokens


def image_data(image_id: int,
               image_filename: str,
               raw_data: dict,
               images_dirname: str,
               codo_id: int):

    assert raw_data["id"] == image_id
    
    split = next(split_choice)
    ouput_dirname = f"{images_dirname}_{split}"
    
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

    _, _, filenames = next(os.walk(image_dir))
    for i, image_filename in enumerate(filenames):
        image_id, _ = path.splitext(image_filename)

        raw_image_data = load_raw_image_data(media_dir, image_id)

        if not raw_image_data:
            logger.warning(f"Failed to retrive scraped data for image {image_id}")
            continue

        coco_data = image_data(int(image_id),
                               image_filename,
                               raw_image_data,
                               images_dirname,
                               coco_id=i)
        logger.info(f"Generated COCO data from scraped data for image {image_id}")
        
        yield coco_data


def copy_images_to_dataset(data: json,
                           from_dir: str,
                           to_dir: str,
                           logger: logging.Logger):

    split_dirname = data["filepath"]
    split_subdirectory = path.join(to_dir, split_subdirectory)      
    # Create directory if absent
    Path(split_subdirectory).mkdir(parents=True, exist_ok=True)

    filename = data["filename"]
    from_path = path.join(from_dir, filename)
    to_path = path.join(split_subdirectory, filename)
    
    logger.info(f"Copying {from_path} to {to_path}")
    shutil.copyfile(in_path, out_path)


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
    type=click.Path(exists=True, file_okay=False),
    required=True
)
@click.option('--dataset-name', '-n', type=str, required=True)
@click.option('--log-level', '-l', type=str, default='DEBUG')
def make_coco_dataset(images_dir, media_dir, output, dataset_name, log_level):
    
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)
    
    all_image_data = list(image_data(images_dir, media_dir, logger))
    
    for image_datum in all_image_data:
        copy_image_to_dataset(image_datum, images_dir, output_dir)
    
    coco_data = {"dataset": dataset_name, "images": data}
    coco_data_path = path.join(output_dir, "data.json")
    json.dump(complete_data, coco_data_path)


if __name__ == "__main__":
    make_coco_dataset()

