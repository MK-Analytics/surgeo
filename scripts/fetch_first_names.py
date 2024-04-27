import io
import pathlib
import urllib.request

import numpy as np
import openpyxl
import pandas as pd

HARVARD_FIRST_NAME_URL = 'https://dataverse.harvard.edu/api/access/datafile/3078263?format=original&gbrecs=true'

HARVARD_FIRST_NAME_COLUMNS = [
    'firstname',
    'obs',
    'pcthispanic',
    'pctwhite',
    'pctblack',
    'pctapi',
    'pctaian',
    'pct2prace',
    'NaN',
]

TARGET_FIRST_NAME_COLUMNS = [
    'firstname',
    'pctwhite',
    'pctblack',
    'pctapi',
    'pctaian',
    'pct2prace',
    'pcthispanic',
]

TARGET_COLUMN_RENAMES = {
    'firstname': 'name',
    'pcthispanic': 'hispanic',
    'pctwhite': 'white',
    'pctblack': 'black',
    'pctapi': 'api',
    'pctaian': 'native',
    'pct2prace': 'multiple',
}

def url_to_df(url):
    '''Takes the URL of a the Harvard first name excel file and converts to DF
    '''
    # Download zipfile from census URL
    
    import urllib.request
    req = urllib.request.Request(
        url, 
        data=None, 
        headers={
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
        }
    )
    f = urllib.request.urlopen(req)
    excel_data = io.BytesIO(f.read())
    ef = openpyxl.load_workbook(excel_data)
    
    #Extracting the values from the openpyxl object (generator) and then unpacking the generator into a list of tuples
    ws_data = ef['Data'].values   
    unpacked_gen = [x for x in ws_data]

    # The first iteration of the extracted tuple list is the column, read that in appropriately and the rest in as data
    df = pd.DataFrame(unpacked_gen[1:], columns=unpacked_gen[0])
    
    return df

def clean_df(df:pd.DataFrame) -> pd.DataFrame:
    '''Change column names, set index, and convert percentages'''
    # Change names
    df.columns = HARVARD_FIRST_NAME_COLUMNS
    # Filter columns
    df = df[TARGET_FIRST_NAME_COLUMNS]
    # Rename columns
    df = df.rename(columns=TARGET_COLUMN_RENAMES)
    # Set index to name
    df = df.set_index('name')
    # Sort index
    df = df.sort_index()
    # Convert percentages to 0 to 1 numbers
    df = df / 100
    return df

def main() -> None:

    # DOWNLOAD FIRST NAME DATA
    print('Downloading data ....')
    df_harvard = url_to_df(HARVARD_FIRST_NAME_URL)

    # CLEAN DATA
    df_harvard = clean_df(df_harvard)
    df_harvard = df_harvard.round(4) # Round to 4 digits

    # WRITE DATA TO MODULE AS CSV
    current_directory = pathlib.Path().cwd()
    project_directory = current_directory.parents[0]
    data_directory    = project_directory / 'surgeo' / 'data'
    path_harvard      = data_directory / 'prob_race_given_first_name_harvard.csv'
    df_harvard.to_csv(path_harvard)

    # CREATING THE REVERSE MAPPING DATA
    column_totals = df_harvard.sum(axis=0).divide(100.0)
    ratio_by_column = df_harvard.divide(column_totals, axis='columns').copy()
    # Prob first name given race
    rbc_path = data_directory / 'prob_first_name_given_race_harvard.csv'
    ratio_by_column.to_csv(rbc_path)

    print('.... Download complete')

if __name__ == '__main__':
    
    main()