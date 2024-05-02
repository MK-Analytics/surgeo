import io
import pathlib
import urllib.request
import zipfile

import numpy as np
import pandas as pd

#CENSUS_URL_2000 = 'https://www2.census.gov/topics/genealogy/2000surnames/names.zip'

CENSUS_URL_2010 = 'https://www2.census.gov/topics/genealogy/2010surnames/names.zip'

CENSUS_SURNAME_COLUMNS = [
    'name',
    'rank',
    'count',
    'proportion',
    'cum_proportion',
    'white',
    'black',
    'api',
    'native',
    'multiple',
    'hispanic',
]

TARGET_SURNAME_COLUMNS = [
    'name',
    'white',
    'black',
    'api',
    'native',
    'multiple',
    'hispanic',
]

def url_to_df(url):
    '''Takes the URL of a Census zip file and converts to DF
    
    Note: it appears the Census webservers rate limit this so it
    may take some time.
    
    '''
    # Download zipfile from census URL
    with urllib.request.urlopen(url) as response:
        # Write file into BytesIO object
        zip_data = io.BytesIO(response.read())
        # Open zip data as zipfile
        with zipfile.ZipFile(zip_data) as zf:
            # Filter out everything except the ZipInfo for csv we want
            csv_info = [file for file in zf.filelist if '.csv' in file.filename][0]
            # Read that CSV into BytesIO object
            raw_data = io.BytesIO(zf.read(csv_info))
            # Create dataframe with only suppressed '(S)' converted to NA
            df = pd.read_csv(raw_data, na_values='(S)', keep_default_na=False)
            return df
        
df_2010 = url_to_df(CENSUS_URL_2010)

df_2010.tail()

# CLEAN DATA

def clean_df(df):
    '''Change column names, set index, and convert percentages'''
    # Change names
    df.columns = CENSUS_SURNAME_COLUMNS
    # Filter columns
    df = df[TARGET_SURNAME_COLUMNS]
    # Set index to name
    df = df.set_index('name')
    # Sort index
    df = df.sort_index()
    # Convert percentages to 0 to 1 numbers
    df = df / 100
    return df

df_2010 = clean_df(df_2010)

# Unsuppress/Imput Anonymized Data
def unsuppress_row(row):
    '''Apply function to desuppress data on row-basis
    
    If a percentage falls beneath a certain threshold, the
    US Census quasi-anonymizes it by supressing. To impute
    new values, we get the outstanding allocated percentage
    and divy it up among the suppressed fields.
    
    '''
    # Check if row has NA values
    if row.isna().sum() > 0:
        # Get count of NA values
        na_count = row.isna().sum()
        # Get total of percentages
        row_sum = row.sum()
        # Get unallocated percentage and divide by count of NA
        na_value = (1 - row_sum) / na_count
        # Fill NA values with that row value.
        reconstituted_row = row.fillna(na_value)
        # Round if necessary
        return reconstituted_row.round(4)
    else:
        # If there's no NA, there's no need to impute
        return row
    
# Get rows with NaNs
target_2010 = df_2010.isna().any(axis=1)
# Run this inefficient operation on rows with NaNs only
df_2010.loc[target_2010] = (
    df_2010.loc[target_2010].apply(unsuppress_row, axis=1)
)

# Round to 4 digits
df_2010 = df_2010.round(4)

# Write Data to Module as CSV
current_directory = pathlib.Path().cwd()
project_directory = current_directory.parents[0]
data_directory    = project_directory / 'surgeo' / 'data'
path_2010         = data_directory / 'prob_race_given_surname_2010.csv'
df_2010.to_csv(path_2010)