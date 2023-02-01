#!/usr/bin/env python
"""
Module to organize helper functions to handle MongoDB data
"""

from omegaconf import OmegaConf
from pymongo import MongoClient

# import pandas as pd


def data_insertion(dataframe, client, collection):
    """
    Check if value inputs are in the same schema as it is suppose to be
    """
    insert = dataframe.to_dict(orient='index') #records
    insert = list(insert.values())
    client[collection].insert_many(insert)

def schema_checker(dataframe):
    """
    Check if value inputs are in the same schema as it is suppose to be
    """
    run_conf = OmegaConf.load('./fields_naming.yaml')
    format_dict = OmegaConf.to_object(run_conf)
    exp_columns = format_dict['exp_fields']
    if set(exp_columns) == set(dataframe.columns):
        # Returns True if file matches schema
        return True        
    return False


def mongo_client(config_file_path):
    """Funcion to turn mongo client ON"""
    config = OmegaConf.load(config_file_path)
    server_ip = config.main.server_ip
    port = config.main.port
    if config.main.login:
        user = config.main.user
        passwd = config.main.passwd
        client = MongoClient(f'mongodb://{server_ip}:{port}',
                            username=user,
                            password=passwd)
    else:
        client = MongoClient(f'mongodb://{server_ip}:{port}')

    return client.TV_scan


def new_register_check(client, collection_name, spotid_field, check_list):
    """Helper function to gather the date from the last Spot Register
    present in MongoDB/TV_scan/Spots_Collection"""
    cursor = client[collection_name].find()
    id_list = []
    for doc in cursor:
        id_list.append(doc[spotid_field])
    
    checker = list(set(check_list) - set(id_list))
    return checker




if __name__ == "__main__":
    pass
