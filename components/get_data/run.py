"""
MLflow component to get Stellantis GA data and upload to a GCStorage bucket
"""

import argparse
import logging
import tempfile

from omegaconf import OmegaConf
import wandb

from helpers.gdrive_helper import download_file
from helpers.email_helper import retrieve_report_email
from helpers.bq_helper import post_csv_to_bq


# basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def get_data(argparse_args):
    """Function to connect to ftp, get the files, log and save in W&B."""
    api_key = argparse_args.wandblogin
    wandb.login(key=api_key)
    with wandb.init(job_type="get_data", project=argparse_args.project) as run:
        logger.info("Process Started.")
        file_reg = {}
        conf_yaml = OmegaConf.load('/conf/run_info.yaml')
        run_conf = OmegaConf.to_object(conf_yaml)
        run.config.update({'input_conf': run_conf})
        logger.info("Config file loaded with %i item(s).", len(run_conf.items()))
        # For each job in the run_info file
        file_reg = {}
        for item in run_conf.items():
            run_name, run_param = item
            # Go to LAST ONE email and retrieve IDs
            fileid = retrieve_report_email(run_param['mail_folder'], 1)[0]
            file_loc = download_file(fileid, run_param)
            # Get each file by ID
            # Send each one to GBQ
            post_info = post_csv_to_bq(file_loc,
                                       run_param['gcp_project'],
                                       run_param['bq_dest_table'],
                                       run_param['columns'])
            file_reg[run_param['brand']] = post_info

        wandb.log({'files_downloaded':file_reg})

        # Upload artifacts
        
        logger.info("All Files loaded in BQ.")
        return run.id


if __name__ == "__main__":
    # Argparse is being used to imput dynamic values programmed in MLproject file.
    # customize the arguments as you want.
    parser = argparse.ArgumentParser(description="Get FTP files and save locally")

    parser.add_argument("project", type=str, help="Name of Wandb project")

    parser.add_argument(
        "wandblogin", type=str, help="Wandb login key"
    )

    args = parser.parse_args()
    # Here you can add all steps this pipe need to accomplish this task.
    get_data(args)
