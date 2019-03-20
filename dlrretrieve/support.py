#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Support functions for the dlrretrieve module
"""

import os
from pathlib import Path
import datetime as dt

#Data structure
home_dir = Path.home()
data_dir = os.path.join(home_dir, 'dlr_data')
usr_dir = os.path.join(home_dir, 'dlr_data','usr')
obs_dir = os.path.join(data_dir, 'observations')
table_dir = os.path.join(obs_dir, 'tables')
profiles_dir = os.path.join(obs_dir, 'profiles')
rawprofiles_dir = os.path.join(profiles_dir, 'raw')

for d in [usr_dir, table_dir, rawprofiles_dir]:
    os.makedirs(d, exist_ok=True)

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
    """
    Checks if year range is valid. Valid years are between 1994 and 2014.
    """
    
    for year in args:
        if year >= 1994 and year <= 2014:
            pass
        else:
            raise InputError([year], 'Year is out of range. Please select a year between 1994 and 2014')           
    return

def writeLog(log_line, file_name):    
    """
    Adds timestamp column to dataframe, then write dataframe to csv log file. 
    """
    
    #Create log_dir and file to log path
    log_dir = os.path.join(usr_dir, 'logs')
    os.makedirs(log_dir , exist_ok=True)
    log_path = os.path.join(log_dir, file_name+'.csv')
    
    #Add timestamp
    log_line.insert(0, 'timestamp', dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    #Write log data to file
    if os.path.isfile(log_path):
        log_line.to_csv(log_path, mode='a', header=False, columns = log_line.columns, index=False)
        print('Log entries added to log/' + file_name + '.csv\n')
    else:
        log_line.to_csv(log_path, mode='w', columns = log_line.columns, index=False)
        print('Log file created and log entries added to log/' + file_name + '.csv\n')    
    return log_line
