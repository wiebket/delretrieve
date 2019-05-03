#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 19 14:17:10 2018

@author: SaintlyVi
"""
from setuptools import setup, find_packages
import os
from pathlib import Path

usr_dir = os.path.join(Path.home(), 'del_data','usr')
os.makedirs(usr_dir, exist_ok=True)

setup(
      name='delretrieve',
      version=0.1,
      description='Fetches data from a DLR MSSQL database',
      long_description='This module helps researchers with limited\
      SQL knowledge to extract data from the Domestic Load Research\
      database and store it on disk in a directory structure for local\
      processing.',
      keywords='domestic load research south africa data access',
      url='https://github.com/wiebket/delretrieve',
      author='Wiebke Toussaint',
      author_email='wiebke.toussaint@gmail.com',
      license='CC-BY-NC',
      install_requires=['pandas','numpy','pyodbc','feather-format','plotly','pathlib'],
      include_package_data=True,
      packages=find_packages(),      
      py_modules = ['delretrieve.retrieve_del'],
      data_files=[(os.path.join(usr_dir), [os.path.join(
                  'delretrieve','data', f) for f in [files for root, dirs, files 
                    in os.walk(os.path.join('delretrieve','data'))][0]])
                    ],
      entry_points = {
			'console_scripts': ['delretrieve_data=delretrieve.command_line:main'],
    		}
      )
