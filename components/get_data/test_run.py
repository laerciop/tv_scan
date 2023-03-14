#!/usr/bin/env python
"""
Pytest test
"""

import pytest
from omegaconf import OmegaConf

from .run import get_data
from helpers.mongo_handler import mongo_client

# Module TO BE Developed

@pytest.fixture(scope='module')
def run_inputs():
    """Pytest fixture for data to test"""
    param = OmegaConf.create("""
                             project: tv_scan_test
                             file_name: test_input.xlsx
                             mongo_config_path: /conf/mongo-config.yaml
                             collection_name: spots_inventory
                             job_reference: test_job_1914
                             wandblogin: ea7d4ed782356f66850586612f115565b6a8a0c3
                            """)
    return param


def test_get_data(run_inputs):
    """Function to test get data outputs"""
    bd = mongo_client(run_inputs.mongo_config_path)
    # insert test register
    test_reg = {'SpotCode': 999999,
                'SpotDesc': 'AJUDANDO A RESGATAR A AUTOESTIMA E SALVANDO VIDAS',
                'VehicleDesc': 'TV Record',
                'BrandDesc': 'BARIATRIC ISTANBUL',
                'SubBrandDesc': 'None',
                'SectorDesc': 'SERVICOS PESSOAIS',
                'CategoryDesc': 'BELEZA E BEM ESTAR',
                'ClassDesc': 'CLINICAS DE ESTETICA/EMAGRECIMENTO',
                'SubClassDesc': 'None',
                'AdvertiserDesc': 'BARIATRIC ISTANBUL',
                'MediaFile': 402883,
                'MediaFileOldUrl': 'http://e-sol.mediamonitor.pt/SOLFILES/TELEVISAO/20220101/402883.wmv'}

    bd['spots_inventory'].insert_one(test_reg)
    # insert test job
    reg = bd['spots_inventory'].find_one({'SpotCode':999999})
    bd['Proc_Jobs'].insert_one({'doc_list': [reg['_id']],
                                'j_ref':run_inputs.job_reference,
                                'status':'Pending Processing.'})
    get_data(run_inputs)
    # check test job output
    # file saved in GCS
    # spot_reg updated with the right URL
    # Proc_jobs updated with the job status
    



if __name__ == "__main__":
    pass
