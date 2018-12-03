# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Support functions for the src module
"""

import os
from pathlib import Path
import datetime as dt

# root dir
dlrdb_dir = str(Path(__file__).parents[0])

# level 1
obs_dir = os.path.join(dlrdb_dir, 'observations')
data_dir = os.path.join(dlrdb_dir, 'data')

# level 2 & 3 DATA
table_dir = os.path.join(data_dir, 'obs_datasets', 'tables')
profiles_dir = os.path.join(data_dir, 'obs_datasets', 'profiles')

# level4 data
rawprofiles_dir = os.path.join(profiles_dir, 'raw')
aggprofiles_dir = os.path.join(profiles_dir, 'aggProfiles')

class InputError(ValueError):
    """Exception raised for errors in the input.

    Attributes:
        expression -- input expression in which the error occurred
        message -- explanation of the error
    """

    def __init__(self, expression, message):
        self.expression = expression
        self.message = message
        
def validYears(*args):
    for year in args:
        if year >= 1994 and year <= 2014:
            pass
        else:
            raise InputError([year], 'Year is out of range. Please select a year between 1994 and 2014')           
    return

