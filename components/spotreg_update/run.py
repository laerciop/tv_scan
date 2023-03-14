#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning,
exporting the result to a new artifact
"""
import argparse
import logging

import wandb
import pandas as pd

from helpers.mongo_handler import mongo_client, schema_checker, new_register_check, data_insertion


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def spot_register_update(args):
    """This function takes a Spot Register file placed in the ./data_in folder reads it and
    send to MongoDB server only the registers that are new.
    (WRITE WHAT IS CONSIDERED "new")
    """
    api_key = args.wandblogin
    wandb.login(key=api_key)

    with wandb.init(job_type="spot_reg_update", project=args.project) as run:
        run.config.update(args)
        input_file_path = "/data_in/"+args.file_name
        spots_inv_coll = args.spots_inventory
        try:
            data_in = pd.read_excel(input_file_path)
            if not schema_checker(data_in): # check if DF is the Mediamonitor one
                raise ImportError
            artifact = wandb.Artifact(name="TV_Spot_Register",
                                      type='Data In',
                                      description='Marktest file')
            artifact.add_file(input_file_path)
            run.log_artifact(artifact)
            logger.info("File %s loaded...", args.file_name)
        except (ImportError, wandb.errors.CommError):
            logger.warning("File input not matching expected schema...")
            return None
        # Dataframe cleaning and prep
        data_in = data_in.drop_duplicates(subset='SpotCode', keep='first')
        keep_cols = ['SpotCode',
                    'SpotDesc',
                    'VehicleDesc',
                    'BrandDesc',
                    'SubBrandDesc',
                    'SectorDesc',
                    'CategoryDesc',
                    'ClassDesc',
                    'SubClassDesc',
                    'AdvertiserDesc',
                    'MediaFile',
                    'MediaFileOldUrl']

        data_in = data_in.loc[:,keep_cols]

        # Loading mongo conf
        tv_scan_db = mongo_client(args.mongo_config_path)
        logger.info("TV Scan DB config loaded...")
        # CHECK and filter new registers
        try:
            checker = new_register_check(tv_scan_db,
                                         spots_inv_coll,
                                         'SpotCode',
                                         list(data_in['SpotCode']))
            if len(checker) > 0:
                # INSERT filtered registers
                insert_df = data_in.loc[data_in['SpotCode'].isin(checker)]
                data_insertion(insert_df, tv_scan_db, spots_inv_coll)
                logger.info("New %i registers inserted in Mongo...", len(checker))
            else: 
                logger.info("No new spots found, no new spots were inserted")
                return None
            
        except FileNotFoundError: # USED A BIZARRE EXEPTION TO CATCH THE ONE THAT I REALLY TO EXCEPT
            data_insertion(data_in, tv_scan_db)
            logger.info("All data inserted in Mongo...")
            return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Update spots register in mongodb")

    parser.add_argument("project", type=str, help="Name of Wandb project")

    parser.add_argument("file_name",
                        type=str, help="Name of Spot Register file placed in ./data_in")

    parser.add_argument("mongo_config_path",
                        type=str,
                        help="the path of the mongo server config file")

    parser.add_argument("spots_inventory",
                        type=str,
                        help="name of the collection of spots in MongoDB")

    parser.add_argument("--wandblogin",
                        type=str,
                        help="Wandb API key",
                        required=True)

    argparse_args = parser.parse_args()

    spot_register_update(argparse_args)
