#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: Wiebke Toussaint

Comman line interface for dlrretrieve module.

Updated: 22 March 2019
"""

from optparse import OptionParser
from .retrieve_dlr import saveTables, saveAnswers, saveRawProfiles
from .support import validYears

def main():
    parser = OptionParser()
    
    parser.add_option('-p', '--profiles', action='store_true', dest='profiles', 
                      help='Save profiles to disk')    
    parser.add_option('-t', '--tables', action='store_true', dest='tables', 
                      help='Save tables to disk')
    parser.add_option('-s', '--surveys', action='store_true', dest='answers', 
                      help='Save anonymised survey responses to disk')
    parser.add_option('-y', '--startyear', dest='startyear', type=int, 
                      help='Start year for profile data retrieval')
    parser.add_option('-z', '--endyear', dest='endyear', type=int, 
                      help='End year for profile data retrieval')
    parser.add_option('-c', '--csv', action='store_true', dest='csv', 
                      help='Save profiles as .csv files.')
    
    parser.set_defaults(tables=False, answers=False, profiles=False, csv=False)
    
    (options, args) = parser.parse_args()
        
    if options.tables == True:
        saveTables()
        
    if options.answers == True:
        saveAnswers()
    
    if options.csv == True:
        filetype = 'csv'
    else:
        filetype = 'feather'
    
    if options.profiles == True:
        if options.startyear is None:
            options.startyear = int(input('Enter observation start year: '))
        if options.endyear is None:
            options.endyear = int(input('Enter observation end year: '))
            
        validYears(options.startyear, options.endyear)   #check that year input is valid     
        saveRawProfiles(options.startyear, options.endyear, filetype)
   
    return print('>>>Data retrieve complete.<<<')
    