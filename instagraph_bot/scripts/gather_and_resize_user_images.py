from datetime import datetime
import json

import click
from os import path, walk
from PIL import Image, ImageOps
import yaml

from util import initialise_logger


def image_paths_and_ids(directory: str, oldest: datetime, logger):
    for dirpath, _, filenames in walk(directory):
        if "data.json" not in filenames:
            continue
        
        image_id = path.basename(path.normpath(dirpath))
        image_path = path.join(dirpath, "image.jpg")
        
        with open(path.join(dirpath, "data.json")) as file_obj:
            data = json.load(file_obj)
        date_taken = datetime.utcfromtimestamp(data["taken_at_timestamp"])
        
        if date_taken < oldest:
            logger.info(f"Skipping image {image_id}: date taken is {date_taken.isoformat()}")
    
        if "image.jpg" in filenames:
            yield image_path, image_id


def resize_and_save(output_directory: str,  image_path: str, image_id: str, image_index: int, resolution: int, logger):
    try:
        output_path = path.join(output_directory, f"{image_id}.jpg")

        if path.isfile(output_path):
            logger.info(f"{image_index}: Image already exists. Skipping {image_path}")
            return

        logger.info(f"{image_index}: Loading image {image_path}")
        image = Image.open(image_path)

        if image.mode == "L":
            image = image.convert("RGB")

        resized_image = ImageOps.fit(image, (resolution, resolution), Image.BICUBIC)

        output_path = path.join(output_directory, f"{image_id}.jpg")
        logger.info(f"{image_index}: Saving to {output_path}")
        resized_image.save(output_path)
    except:
        logger.exception("{image_index} Image is wierd! Skipping... ¯\_(ツ)_/¯")


@click.command()
@click.argument("users_directory")
@click.argument("output_directory")
@click.option("-u", "--user-ids", default="", help="Comma-seperated list of user ids.")
@click.option("-o", "--oldest", default=None, help="ISO-formatted date of oldest image.")
@click.option("-r", "--resolution", default=1024, help="Output resolution of (square) images.")
@click.option("--dry-run", is_flag=True)
@click.option('--log-level', '-l', type=str, default='DEBUG')
def gather_and_resize(users_directory, output_directory, user_ids, oldest, resolution, dry_run,  log_level):
    with open('config.yaml') as file_obj:
        config = yaml.safe_load(file_obj)

    file_friendly_datetime = datetime.now().strftime('%Y-%m-%d_%H%M')
    logger = initialise_logger(
        directory=config['logs_directory'],
        name=f'{file_friendly_datetime}_gather-images',
        module='instagraph_bot.scripts.gather_and_resize_images',
        level=log_level,
    )

    oldest = datetime.fromisoformat(oldest)
    
    total_images = 0

    if not user_ids:
        logger.info("No user ids specified. Gathering images for all users...")
        
        for i, (image_path, image_id) in enumerate(image_paths_and_ids(users_directory, oldest, logger), 1):
            total_images += 1

            if not dry_run:
                resize_and_save(output_directory, image_path, image_id, i, resolution,  logger)
        
    for user_id in (i.strip() for i in user_ids.split(",") if i.strip()):
        user_path = path.join(users_directory, user_id)
        logger.info(f"Getting images for user {user_id}...")
        
        for i, (image_path, image_id) in enumerate(image_paths_and_ids(user_path, oldest, logger), 1):
            total_images += 1

            if not dry_run:
                resize_and_save(output_directory, image_path, image_id, i, resolution, logger)

    logger.info(f"Total image count: {total_images}")


if __name__ == "__main__":
    gather_and_resize()

