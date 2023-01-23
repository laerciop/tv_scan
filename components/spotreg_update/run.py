#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning,
exporting the result to a new artifact
"""
import argparse
import logging
import os

from omegaconf import OmegaConf
import wandb
import pandas as pd

from helpers.mongo_handler import mongo_client


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def spot_register_update(args):
    """This function takes a Spot Register file placed in the ./data_in folder reads it and
    send to MongoDB server only the registers that are new.
    (WRITE WHAT IS CONSIDERED "new")
    """
    api_key = argparse_args.wandblogin
    wandb.login(key=api_key)

    with wandb.init(job_type="test_upload", group="test_upload", project="test_project") as run:
        try:
            input_artifact = run.use_artifact(args.artifact_name+':latest', type='st_cleaned')
            run.config.update({'child_artifact':input_artifact.json_encode()})
        except (ValueError, wandb.errors.CommError):
            logger.warning("Artif %s not found in laerciop...")
        run.config.update(argparse_args)
        tv_scan = mongo_client(argparse_args.mongo_config_path)
        logger.info("TV Scan DB config loaded...")
        data_in = pd.read_excel("/data_in/"+args.file_name)
        logger.info("File %s loaded...", args.file_name)
        # CHECK file schema
        # CHECK and filter new registers
        # INSERT filtered registers
        # LOG in W&B


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Tests database and upload it to NLP tank")

    parser.add_argument("project", type=str, help="Name of Wandb project")

    parser.add_argument("file_name",
                        type=str, help="Name of Spot Register file placed in ./data_in")

    parser.add_argument("--mongo_config_path",
                        type=str,
                        help="the path of the mongo server config file")

    parser.add_argument("--wandblogin",
                        type=str,
                        help="Wandb API key",
                        required=True)

    argparse_args = parser.parse_args()

    spot_register_update(argparse_args)
