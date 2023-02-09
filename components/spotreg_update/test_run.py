#!/usr/bin/env python
"""
Pytest test
"""

import pytest

from omegaconf import OmegaConf
from .run import spot_register_update
from helpers.mongo_handler import mongo_client

@pytest.fixture(scope='module')
def run_inputs():
    """Pytest fixture for data to test"""
    param = OmegaConf.create("""
                             project: tv_scan_test
                             file_name: test_input.xlsx
                             mongo_config_path: /conf/mongo-config.yaml
                             spots_inventory: spots_inventory
                             wandblogin: ea7d4ed782356f66850586612f115565b6a8a0c3
                            """)
    return param


def test_get_data(run_inputs):
    """Function to test spot_register_update"""
    bd = mongo_client(run_inputs.mongo_config_path)
    spot_register_update(run_inputs)
    test_ids = [99, 999, 9999, 99999, 999999]
    cursor = bd['spots_inventory'].find({'SpotCode':{'$in':test_ids}})
    check_list = []
    for doc in cursor:
        check_list.append(doc['SpotCode'])
    assert len(check_list) == 5 # assert that the list have the expected number
    bd['spots_inventory'].delete_many({'SpotCode':{'$in':test_ids}}) # clear BD



if __name__ == "__main__":
    pass
