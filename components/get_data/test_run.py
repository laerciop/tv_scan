#!/usr/bin/env python
"""
Pytest test
"""
import os
import pytest
from omegaconf import OmegaConf
from .run import get_data
import wandb

# Module TO BE Developed

@pytest.fixture(scope='module')
def run_inputs():
    """Pytest fixture for data to test"""
    param = OmegaConf.create("""
                             project: test
                            """)
    return param


def test_get_data(run_inputs):
    """Function to test get data outputs"""
    # get_data('args')
    pass



if __name__ == "__main__":
    pass
