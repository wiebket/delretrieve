#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Support functions for the src module
"""

import os
from pathlib import Path

#Data structure
home_dir = Path.home()
data_dir = os.path.join(home_dir, 'dlr_data')
obs_dir = os.path.join(data_dir, 'observations')
table_dir = os.path.join(obs_dir, 'tables')
profiles_dir = os.path.join(obs_dir, 'profiles')
rawprofiles_dir = os.path.join(profiles_dir, 'raw')

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

