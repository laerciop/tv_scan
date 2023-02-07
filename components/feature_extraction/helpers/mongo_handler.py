#!/usr/bin/env python
"""
Module to organize helper functions to handle MongoDB data
"""
import tempfile

import numpy as np

from omegaconf import OmegaConf
from pymongo import MongoClient, UpdateOne
from .file_handlers import FileHandler
from .transcriptor import TranscriptorModel



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



def field_checker(field_to_check, document):
    """Function to check if the selected field was already processed in this document.
    Input:
        - Field_to_check: name of the field you want to check
    Output:
        - True if field is empty"""

    if field_to_check in document:
        return True # returns TRUE if there's already a field in there
    else:
        return False 


def get_processing_list(job_reference, client, collection):
    """Function to help getting spot processing list"""
    job = client[collection].find_one({"j_ref":job_reference})
    query = {'_id':{'$in':job['doc_list']}}
    return query


def update_processing_status(new_status, job_reference, client, collection):
    """Function to help changing job status"""
    job = client[collection].find_one({"j_ref":job_reference})
    client[collection].update_one({'_id':job['_id']},{'$set':{'status':new_status}} )
    return job['doc_list']


def core_processor_helper(client, collection, query, flag_field, model_type, wb_run_obj):
    """Helper to run the core text processing task defined in the processing_pipe function"""
    cursor = client[collection].find(query)
    reg_sample = cursor.next()
    cursor.rewind()
    wb_run_obj.config.update({'processing_sample':reg_sample})
    gcs_handler = FileHandler('publicispt-datastage')
    model = TranscriptorModel(model_type)
    # List of all DB updates set to happen
    db_updates = []
    for doc in cursor:
        if field_checker(flag_field, doc): # returns TRUE if there's file available
            new_feature_name = 'audtrans_'+model_type
            if field_checker(new_feature_name, doc): # returns TRUE if there's already processing
                continue
            doc_updates = {}
            with tempfile.NamedTemporaryFile(mode='w+b') as tmp:
                gcs_handler.spot_gather_helper(doc['GCP_path'], tmp.name)
                result = model.transcriptor_helper(tmp.name)
            doc_updates[new_feature_name] = result['text']
            doc_updates['audtrans_lang'] = result['language']
            comp_rate = np.average([x['compression_ratio'] for x in result['segments']])
            temp = np.average([x['temperature'] for x in result['segments']])
            log_prob = np.average([x['avg_logprob'] for x in result['segments']])
            nospe_prob = np.average([x['no_speech_prob'] for x in result['segments']])
            wb_run_obj.log({'comp_rate':comp_rate,
                            'temp':temp,
                            'log_prob':log_prob,
                            'nospe_prob':nospe_prob})

            db_updates.append(UpdateOne({'_id': doc['_id']}, {'$set': doc_updates}))
    if len(db_updates) > 0:
        client[collection].bulk_write(db_updates)
    return db_updates



if __name__ == "__main__":
    pass
