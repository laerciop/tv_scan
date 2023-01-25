#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning,
exporting the result to a new artifact
"""
import argparse
import logging

import wandb
import pandas as pd

from helpers.mongo_handler import mongo_client, schema_checker, last_spot_date_mongo, data_insertion


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def spot_register_update(args):
    """This function takes a Spot Register file placed in the ./data_in folder reads it and
    send to MongoDB server only the registers that are new.
    (WRITE WHAT IS CONSIDERED "new")
    """
    api_key = argparse_args.wandblogin
    wandb.login(key=api_key)

    with wandb.init(job_type="Spot Register Update", group="Update", project=argparse_args.project) as run:
        run.config.update(argparse_args)
        input_file_path = "/data_in/"+args.file_name
        try:
            data_in = pd.read_excel(input_file_path)
            if not schema_checker(data_in):
                raise ImportError
            artifact = wandb.Artifact(name="TV_Spot_Register",
                                      type='Data In',
                                      description='Marktest file')
            artifact.add_file(input_file_path)
            run.log_artifact(artifact)
            logger.info("File %s loaded...", args.file_name)
        except (ImportError, wandb.errors.CommError):
            logger.warning("File input not matching expected schema...")
        
        # Loading mongo conf
        tv_scan_db = mongo_client(argparse_args.mongo_config_path)
        logger.info("TV Scan DB config loaded...")
        # CHECK and filter new registers
        last_mongo_date = last_spot_date_mongo(tv_scan_db,
                                               'test_collection',
                                               'SolDate')
        
        insert_df = data_in.loc[data_in['SolDate'].gt(last_mongo_date)]
        if len(insert_df) < 1:
            logger.info("No new spots found, no new spots were inserted")
            return None
        
        # INSERT filtered registers
        data_insertion(data_in, tv_scan_db)
        logger.info("Data inserted in Mongo...")

        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update spots register in mongodb")

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
