from datetime import datetime
import json
import logging
from random import choices
from typing import List

import click
from nltk.tokenize import RegexpTokenizer
from os import path, walk


COCO_SPLIT_PROPORTIONS = {
    'test': 0.040555776359226844,
    'restval': 0.24742268041237114,
    'val': 0.040555776359226844,
    'train': 0.6714657668691751
}


def split_split_choices():
    population = list(COCO_SPLIT_PROPORTIONS.keys())
    weights = list(COTO_SPLIT_PROPORTIONS.values())
    
    while True:
        yield choices(population, weights=weights)[0]

split_choice = split_choices()
insta_tokeniser = RegexpTokenizer(r"[#@'_\w]+")


def load_raw_image_data(root_dir, image_id):
    for dirpath, _, filenames in walk(root_dir):
        
        if "data.json" not in filenames:
            continue
        
        dir_image_id = path.basename(path.normpath(dirpath))
        if dir_image_id == image_id:

            with open(path.join(dirpath, "data.json"), 'r') as fd:
                return json.load(fd)

def tokenize(caption: str) -> List[str]:
    tokens = insta_tokeniser.tokenise(caption)
    # Remove usernames
    return [token for token in tokens if not token.startswith("@")


def image_data(image_id: int,
               image_filename: str,
               raw_data: dict,
               images_dirname: str,
               codo_id: int):

    assert raw_data["id"] == image_id
    
    split = next(split_choice)
    ouput_dirname = f"{images_dirname}_{split}"
    
    caption = get_caption(raw_data)
    
    sentence = {
        "raw": caption,
        "tokens": insta_tokeniser.tokenise(caption),
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
    "--output-path", 
    "-o", 
    "output", 
    type=click.File("w"), 
    required=True
)
@click.option('--dataset-name', '-n', type=str, required=True)
@click.option('--log-level', '-l', type=str, default='DEBUG')
def gather_and_resize(images_dir, media_dir, output, dataset_name, log_level):
    
    logging.basicConfig(level=log_level)
    logger = logging.getLogger(__name__)

    data = {
        "dataset": dataset_name, 
        "images"=list(image_data(images_dir, media_dir, logger))
    }
    
    json.dump(data, output)

