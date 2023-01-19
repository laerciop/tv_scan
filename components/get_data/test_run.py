#!/usr/bin/env python
"""
Pytest test
"""
import os
import pytest
from omegaconf import OmegaConf
from .run import get_data
import wandb

from helpers.email_helper import retrieve_report_email
from helpers.gdrive_helper import get_file_list
from helpers.gdrive_helper import download_file


@pytest.fixture(scope='module')
def run_inputs():
    """Pytest fixture for data to test"""
    param = OmegaConf.create("""
                             project: unicef_attribuition_test
                             ftp_ip: "213.58.138.122"
                             ftp_user: Starcom
                             ftp_pass: qmTNWXbGLu34p4A3
                             wandblogin: 'ea7d4ed782356f66850586612f115565b6a8a0c3'
                            """)
    return param


def test_get_data(run_inputs):
    """Function to test get data outputs"""
    # get_data('args')
    pass

def test_retrieve_report_email():
    """Function to test if email is being retrieved"""
    test_bucket = 'ste_cmprep_test'
    file_list = retrieve_report_email(test_bucket, 1)
    print(file_list)
    assert '1ztHykB8toSgB_I0NOb1DRSMJgnEB5lyi' in file_list

def test_get_file_list():
    """Function to test if a file list is retrieved from the logged account"""
    file_list = get_file_list(5)
    # assert that there's a list retrieved from the method
    assert isinstance(file_list['files'], list)
    # assert that the list has the size as was requested
    assert len(file_list['files']) == 5

def test_download_file():
    """Function to test if a file is downloaded from the logged account"""
    'temp_file.csv'
    # Known file fileID
    file_id = '1ztHykB8toSgB_I0NOb1DRSMJgnEB5lyi'
    download_file(file_id)
    # check if the file was saved
    assert os.path.exists('temp_file.csv')



if __name__ == "__main__":
    pass
