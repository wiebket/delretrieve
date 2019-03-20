#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

@author: Wiebke Toussaint

This file contains functions to fetch data from the Domestic Load Research SQL Server database. 
It must be run from a server with a DLR database installation or with a connection file that specifies remote access details.
    
"""

import pandas as pd
import numpy as np
import pyodbc 
import feather
import os

from .support import usr_dir, rawprofiles_dir, table_dir, obs_dir, validYears


def getObs(querystring = 'SELECT * FROM tablename', tablename = None, chunksize = 10000):
    """
    Fetches a table from the DLR database and returns it as a pandas dataframe. 
    Requires /src/cnxnstr.txt with database connection parameters.
    """
    
    if tablename == 'Profiletable':
        return print('The profiles table is too large to read into python in one go. \
                     Use the getProfiles() function.')
 
    else:
        #create connection object:
        try:
            with open(os.path.join(usr_dir, 'cnxnstr.txt'), 'r') as f: 
                cnxnstr = f.read().replace('\n', '')
        except FileNotFoundError as err:
            print("Cannot find file with connection information for the database: {0}".format(err))
            raise
            
        try:
            cnxn = pyodbc.connect(cnxnstr)  
            #specify and execute query:
            if querystring == 'SELECT * FROM tablename':
                query = "SELECT * FROM [General_LR4].[dbo].%s" % (tablename)
            else:
                query = querystring  
            df = pd.read_sql(query, cnxn)   #read to dataframe   
            return df
        except Exception:
            raise
            

def getGroups():
    """
    This function performs some massive Groups wrangling to get the groups table into a useable shape.
    """
    #TODO: cache allgroups once retrieved ... or select year in getMetaProfiles only
    
    groups = getObs('Groups')
    groups['ParentID'].fillna(0, inplace=True)
    groups['ParentID'] = groups['ParentID'].astype('int64').astype('category')
    groups['GroupName'] = groups['GroupName'].map(lambda x: x.strip())
    #TRY THIS groups['GroupName'] = groups['GroupName'].str.strip()
    
    #Deconstruct groups table apart into levels
    #LEVEL 1 GROUPS: domestic/non-domestic
    groups_level_1 = groups[groups['ParentID']==0] 
    #LEVEL 2 GROUPS: Eskom LR, NRS LR, Namibia, Clinics, Shops, Schools
    groups_level_2 = groups[groups['ParentID'].isin(groups_level_1['GroupID'])]
    #LEVLE 3 GROUPS: Years
    groups_level_3 = groups[groups['ParentID'].isin(groups_level_2['GroupID'])]
    #LEVLE 4 GROUPS: Locations
    groups_level_4 = groups[groups['ParentID'].isin(groups_level_3['GroupID'])]
    
    #Slim down the group levels to only include columns requried for merging
    g1 = groups.loc[groups['ParentID']==0,['GroupID','ParentID','GroupName']].reset_index(drop=True)
    g2 = groups.loc[groups['ParentID'].isin(groups_level_1['GroupID']), [
            'GroupID','ParentID','GroupName']].reset_index(drop=True)
    g3 = groups.loc[groups['ParentID'].isin(groups_level_2['GroupID']), [
            'GroupID','ParentID','GroupName']].reset_index(drop=True)
    
    #reconstruct group levels as one pretty, multi-index table
    recon3 = pd.merge(groups_level_4, g3, left_on ='ParentID', 
                      right_on = 'GroupID' , how='left', suffixes = ['_4','_3'])
    recon2 = pd.merge(recon3, g2, left_on ='ParentID_3',
                      right_on = 'GroupID' , how='left', suffixes = ['_3','_2'])
    recon1 = pd.merge(recon2, g1, left_on ='ParentID', 
                      right_on = 'GroupID' , how='left', suffixes = ['_2','_1'])
    prettyg = recon1[['ContextID','GroupID_1','GroupID_2','GroupID_3','GroupID_4',
                      'GroupName_1','GroupName_2','GroupName_3','GroupName_4']]
    prettynames = ['ContextID', 'GroupID_1','GroupID_2','GroupID_3','GroupID',
                   'Dom_NonDom','Survey','Year','Location']
    prettyg.columns = prettynames
    
    #create multi-index dataframe
    allgroups = prettyg.set_index(['GroupID_1','GroupID_2','GroupID_3']).sort_index()
    allgroups['LocName'] = allgroups['Location'].apply(lambda x:x.partition(' ')[2])
        
    return allgroups


def getProfileID(year = None):
    """
    Fetches all profile IDs for a given year. None returns all profile IDs.
    """
    
    links = getObs('LinkTable') #TODO cache link table once retrieved
    allprofiles = links[(links.GroupID != 0) & (links.ProfileID != 0)]
    if year is None:
        return allprofiles

    #match GroupIDs to getGroups to get the profile years:
    else:
        validYears(year) #check if year is valid
        allgroups = getGroups()
        groupids = allgroups[allgroups.Year.astype(int) == year, 'GroupID'] 
        profileid = pd.Series(allprofiles.loc[allprofiles.GroupID.isin(groupids),
                                              'ProfileID'].unique())
    return profileid


def getMetaProfiles(year, units = None):
    """
    Fetches profile meta data. Units must be one of  V or A. From 2009 onwards kVA, Hz and kW have 
    also been measured.
    """
    
    #list of profiles for the year:
    pids = pd.Series(map(str, getProfileID(year))) 
    #get observation metadata from the profiles table:
    metaprofiles = (
            getObs('profiles')[['Active','ProfileId','RecorderID','Unit of measurement']])
    metaprofiles = metaprofiles[metaprofiles.ProfileId.isin(pids)] #select subset of metaprofiles corresponding to query
    metaprofiles.rename(columns={'Unit of measurement':'UoM'}, inplace=True)
    metaprofiles.loc[:,['UoM', 'RecorderID']] = (
            metaprofiles.loc[:,['UoM','RecorderID',]].apply(pd.Categorical))
    puom = getObs('ProfileUnitsOfMeasure').sort_values(by=['UnitsID'])
    cats = list(puom.loc[puom.UnitsID.isin(metaprofiles['UoM'].cat.categories), 'Description'])
    metaprofiles['UoM'].cat.categories = cats

    if units is None:
        plist = metaprofiles['ProfileId']
    elif units in ['V','A','kVA','kW']:
        uom = units.strip() + ' avg'
        plist = metaprofiles[metaprofiles.UoM == uom]['ProfileId']
    elif units=='Hz':
        uom = 'Hz'
        plist = metaprofiles[metaprofiles.UoM == uom]['ProfileId']
    else:
        return print('Check spelling and choose V, A, kVA, Hz or kW as units, \
                     or leave blank to get profiles of all.')
    return metaprofiles, plist


def getProfiles(group_year, month, units):
    """
    This function fetches load profiles for one calendar year. 
    It takes the year as number and units as string:
        [A, V] for 1994 - 2008 
        [A, V, kVA, Hz, kW] for 2009 - 2014
    """
    
    # Get metadata
    mp, plist = getMetaProfiles(group_year, units)
    
    ## Get profiles from server
    subquery = ', '.join(str(x) for x in plist)
    query = "SELECT pt.ProfileID \
     ,pt.Datefield \
     ,pt.Unitsread \
     ,pt.Valid \
    FROM [General_LR4].[dbo].[Profiletable] pt \
    WHERE pt.ProfileID IN (" + subquery + ") AND MONTH(Datefield) =" + str(month) + " \
    ORDER BY pt.Datefield, pt.ProfileID"
    profiles = getObs(querystring = query)
    
    #data output:    
    df = pd.merge(profiles, mp, left_on='ProfileID', right_on='ProfileId')
    df.drop('ProfileId', axis=1, inplace=True)
    #convert strings to category data type to reduce memory usage
    df.loc[:,['ProfileID','Valid']] = df.loc[:,['ProfileID','Valid']].apply(pd.Categorical)
    
    head_year = df.head(1).Datefield.dt.year[0]
    tail_year = df.tail(1).Datefield.dt.year[len(df)-1]
    
    return df, head_year, tail_year
    

def writeProfilePath(group_year, year, month, units, filetype):
    """
    This function creates the directory hierarchy and file names for writing raw profiles.
    """
    
    dir_path = os.path.join(rawprofiles_dir, str(group_year), str(year)+'-'+str(month))
    try:
        os.makedirs(dir_path , exist_ok=True) #create profile directory if it does not exist
    except Exception as e:
        return e
    file_path = os.path.join(dir_path, str(year)+'-'+str(month)+'_'+str(units)+'.'+filetype)
    
    return file_path


def writeProfiles(group_year, month, units, filetype):
    """
    This function saves profiles to disk.
    """
    
    df, head_year, tail_year = getProfiles(group_year, month, units)
    head_path = writeProfilePath(group_year, head_year, month, units, filetype)
    tail_path = writeProfilePath(group_year, tail_year, month, units, filetype)
    
    #check if dataframe contains profiles for two years
    if head_year == tail_year: 
        print(head_path)
        try:
            if filetype=='feather':
                feather.write_dataframe(df, head_path)
            elif filetype=='csv':
                df.to_csv(head_path)
            print('Write success')
        except Exception as e:
            print(e)
            pass
    
    #split dataframe into two years and save separately            
    else:
        head_df = df[df.Datefield.dt.year == head_year].reset_index(drop=True)
        print(head_path)
        try:
            if filetype=='feather':
                feather.write_dataframe(head_df, head_path)
            elif filetype=='csv':
                df.to_csv(head_path)
            print('Write success')
        except Exception as e:
            print(e)
            pass
        
        #create directory for second year
        tail_df = df[df.Datefield.dt.year == tail_year].reset_index(drop=True)
        print(tail_path)
        try:
            if filetype=='feather':
                feather.write_dataframe(tail_df, tail_path)
            elif filetype=='csv':
                df.to_csv(tail_path)
            print('Write success')
        except Exception as e:
            print(e)
            pass    
    #write logs
    return


def writeTables(names, dataframes): 
    """
    This function saves a list of names with an associated list of dataframes as feather files.
    The getObs() and getGroups() functions can be used to construct the dataframes.
    
    """
    #create data directories
    os.makedirs(os.path.join(table_dir, 'feather') , exist_ok=True)
    os.makedirs(os.path.join(table_dir, 'csv') , exist_ok=True)

    #get data        
    datadict = dict(zip(names, dataframes))
    for k in datadict.keys():
        if datadict[k].size == datadict[k].count().sum():
            data = datadict[k]
        else:  
            data = datadict[k].fillna(np.nan) #feather doesn't write None type
        
        try:
            feather_path = os.path.join(table_dir, 'feather', k + '.feather')
            feather.write_dataframe(data, feather_path)
            csv_path = os.path.join(table_dir, 'csv', k + '.csv')
            data.to_csv(csv_path, index=False)
            print('successfully saved table '  + k)
        except Exception as e:
            print(e)
            pass            
    return


def saveTables():
    """
    This function fetches tables from the SQL database and saves them as a feather object. 
    """
    
    #Get and save important tables
    groups = getGroups() 
    questions = getObs('Questions')
    questionaires = getObs('Questionaires')
    qdtype = getObs('QDataType')
    qredundancy = getObs('QRedundancy')
    qconstraints = getObs('QConstraints')
    answers = getObs('Answers')
    links = getObs('LinkTable')
    profiles = getObs('Profiles')
    profilesummary = getObs('ProfileSummaryTable')
    recorderinstall = getObs('RECORDER_INSTALL_TABLE')
    
    tablenames = ['groups', 'questions', 'questionaires', 'qdtype', 'qredundancy', 'qconstraints', 'answers', 'links', 'profiles' ,'profilesummary','recorderinstall']
    tabledata = [groups, questions, questionaires, qdtype, qredundancy, qconstraints, answers, links, profiles, profilesummary, recorderinstall]
    
    writeTables(tablenames, tabledata)
    return
 
def saveAnswers():
    """
    This function fetches survey responses and anonymises them to remove all discriminating personal
    information of respondents. The anonymised dataset is returned and saved as a feather object.
    Details for questions to anonymise are contained in two csv files, data/blobAnon.csv and
    data/charAnon.csv.
    """
    
    anstables = {'Answers_blob':'blobAnon.csv', 'Answers_char':'charAnon.csv', 'Answers_Number':None}    
    for k,v in anstables.items():
        a = getObs(k) #get all answers
        if v is None:
            pass
        else:
            qs = pd.read_csv(os.path.join(obs_dir, 'data', v))
            qs = qs.loc[lambda qs: qs.anonymise == 1, :]
            qanon = pd.merge(getObs('Answers'), qs, left_on='QuestionaireID', right_on='QuestionaireID')[['AnswerID','ColumnNo','anonymise']]
            for i, rows in qanon.iterrows():
                a.set_value(a[a.AnswerID == rows.AnswerID].index[0], str(rows.ColumnNo),'a')
        
        writeTables([k.lower() + '_anonymised'],[a]) #saves answers as feather object
    return
    

def saveRawProfiles(yearstart, yearend, filetype='feather'):
    """
    This function iterates through all profiles and saves them in a ordered directory structure by 
    year and unit.
    """
    
    if yearstart < 2009:
        for year in range(yearstart, yearend + 1):
            for unit in ['A','V']:
                for month in range(1, 13):
                    writeProfiles(year, month, unit, filetype)
    elif yearstart >= 2009 and yearend <= 2014:       
        for year in range(yearstart, yearend + 1):
            for unit in ['A', 'V', 'kVA', 'Hz', 'kW']:
                for month in range(1, 13):
                    writeProfiles(year, month, unit, filetype)
    return
