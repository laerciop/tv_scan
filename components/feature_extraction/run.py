"""
MLflow component to extract features from spots
"""

import argparse
import logging

# from omegaconf import OmegaConf
import wandb

from helpers.mongo_handler import core_processor_helper, mongo_client, get_processing_list, update_processing_status

# basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def feature_extraction(argparse_args):
    """Function to connect to mongo, process the video files (already downloaded)
    and save it into DB."""
    api_key = argparse_args.wandblogin
    wandb.login(key=api_key)
    with wandb.init(job_type="get_data", project=argparse_args.project) as run:
        logger.info("Process Started.")
        run.config.update({'run_inputs':argparse_args})
        tv_scan = mongo_client(argparse_args.mongo_config_path)
        logger.info("MongoDB info loaded.")
        # load mongo filter for processing from job referencee
        logger.info("Getting processing job list...")
        proc_job_coll = 'Proc_Jobs' # name of the collection containing the job list
        query = get_processing_list(argparse_args.job_reference,
                                    tv_scan,
                                    proc_job_coll)
        run.config.update({'processing_list':query['_id']['$in']})
        # For each filtered register:
        # # check if there's a downloaded file:
        check_field = 'GCP_path'
        # # # if not, won't try to process
        logger.info("Feature extraction started.")

        processed_list = core_processor_helper(tv_scan,
                                               argparse_args.collection_name,
                                               query,
                                               check_field,
                                               argparse_args.model_type,
                                               run)
        update_processing_status('Download Completed.',
                                 argparse_args.job_reference,
                                 tv_scan,
                                 proc_job_coll)
        run.config.update({'processed_registers':len(processed_list)})
        


if __name__ == "__main__":
    # Argparse is being used to input dynamic values programmed in MLproject file.
    # customize the arguments as you want.
    parser = argparse.ArgumentParser(description="Get spots files to be processed.")
    
    parser.add_argument("project", type=str, help="Name of Wandb project")

    parser.add_argument("collection_name",
                        type=str, help="Mongo db collection where registers are")

    parser.add_argument("mongo_config_path",
                        type=str,
                        help="the path of the mongo server config file")
    
    parser.add_argument("job_reference",
                        type=str,
                        help="name of the job where processing information are stored")
    
    parser.add_argument("model_type",
                        type=str,
                        help="model type to be used in whisper")

    parser.add_argument("--wandblogin",
                        type=str,
                        help="Wandb API key",
                        required=True)

    args = parser.parse_args()
    # Here you can add all steps this pipe need to accomplish this task.
    feature_extraction(args)
