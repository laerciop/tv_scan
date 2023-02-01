#!/usr/bin/env python
"""
Module to organize helper functions to handle MongoDB data
"""

from omegaconf import OmegaConf
from pymongo import MongoClient, UpdateOne
from .file_handlers import FileHandler



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


def last_spot_date_mongo(client, collection_name, date_field):
    """Helper function to gather the date from the last Spot Register
    present in MongoDB/TV_scan/Spots_Collection"""
    doc = client[collection_name].find_one(sort=[(date_field, -1)])
    return doc[date_field]


def field_checker(field_to_check, document):
    """Function to check if the selected field was already processed in this colection
    of NLP tank.
    Input:
        - Field_to_check: name of the field you want to check
    Output:
        - True if field is empty"""

    if field_to_check in document:
        return False
    else:
        return True


def get_processing_list(job_reference, client, collection):
    """Function to help getting spot processing list"""
    job = client[collection].find_one({"j_ref":job_reference})
    query = {'_id':{'$in':job['doc_list']}}
    return query


def update_processing_status(new_status, job_reference, client, collection):
    """Function to help changing job status"""
    job = client[collection].find_one({"j_ref":job_reference})
    client[collection].UpdateOne({'_id':job['_id']},{'$set':{'status':new_status}} )
    return job['doc_list']


def core_processor_helper(client, collection, query, flag_field, wb_run_obj):
    """Helper to run the core text processing task defined in the processing_pipe function"""
    cursor = client[collection].find(query)
    reg_sample = cursor.next()
    cursor.rewind()
    wb_run_obj.config.update({'processing_sample':reg_sample})
    gcs_handler = FileHandler('publicispt-datastage')
    # List of all DB updates set to happen
    db_updates = []
    for doc in cursor:
        if field_checker(flag_field, doc):
            doc_updates = {}
            # Building parameters
            url = doc['MediaFileOldUrl']
            spot_code = str(doc['SpotCode'])
            brand = doc['BrandDesc'].replace(r'\s', '_') # Replace Whitespaces
            brand = doc['BrandDesc'].replace(r'\W', '_') # eliminate non-word
            file_format = doc['MediaFileOldUrl'][-4:]
            dest_name = spot_code+"_"+brand+file_format
            # Download the file if not downloaded yet
            dest_file, file_url = gcs_handler.spot_download_helper(url,
                                                                   dest_name)
            doc_updates[flag_field] = file_url
            doc_updates['spot_store_filename'] = dest_file
            # store Object link in mongo DB in the FIELD
            db_updates.append(UpdateOne({'_id': doc['_id']}, {'$set': doc_updates}))
    
    client[collection].bulk_write(db_updates)
    return db_updates



if __name__ == "__main__":
    pass
