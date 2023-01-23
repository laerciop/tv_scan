#!/usr/bin/env python
"""
Module to organize helper functions to handle MongoDB data
"""

from omegaconf import OmegaConf
from pymongo import MongoClient

# import pandas as pd


def data_insertion(dataframe, client):
    """
    Check if value inputs are in the same schema as it is suppose to be
    """
    insert = dataframe.to_dict(orient='index') #records
    insert = list(insert.values())
    client['spots_inventory'].insert_many(insert)

def schema_checker(dataframe):
    """
    Check if value inputs are in the same schema as it is suppose to be
    """
    run_conf = OmegaConf.load('/app/fields_naming.yaml')
    format_dict = OmegaConf.to_object(run_conf)
    exp_columns = format_dict['exp_fields']
    if set(exp_columns) == set(dataframe.columns):
        return True        
    return False


def mongo_client(config_file_path):
    """Funcion to turn mongo client ON"""
    config = OmegaConf.load(config_file_path)
    server_ip = config.main.server_ip
    port = config.main.port
    if config.login:
        user = config.main.user
        passwd = config.main.passwd
        client = MongoClient(f'mongodb://{server_ip}:{port}',
                            username=user,
                            password=passwd)
    else:
        client = MongoClient(f'mongodb://{server_ip}:{port}')

    return client.TV_scan



if __name__ == "__main__":
    pass
