import io
import pathlib
import urllib.request
import tempfile
import zipfile
from IPython.display import display
import numpy as np
import pandas as pd

# These are the start/stop indices for the fixed width geo file.
# It allows them to be easily converted to dataframes.
GEO_MAP_2010 = {
    'FILEID'  : (1  , 7  ),
    'STUSAB'  : (7  , 9  ),
    'SUMLEV'  : (9  , 12 ),
    'GEOCOMP' : (12 , 14 ),
    'CHARITER': (14 , 17 ),
    'CIFSN'   : (17 , 19 ),
    'LOGRECNO': (19 , 26 ),
    'REGION'  : (26 , 27 ),
    'DIVISION': (27 , 28 ),
    'STATE'   : (28 , 30 ),
    'COUNTY'  : (30 , 33 ),
    'COUNTYCC': (33 , 35 ),
    'COUNTYSC': (35 , 37 ),
    'COUSUB'  : (37 , 42 ),
    'COUSUBCC': (42 , 44 ),
    'COUSUBSC': (44 , 46 ),
    'PLACE'   : (46 , 51 ),
    'PLACECC' : (51 , 53 ),
    'PLACESC' : (53 , 55 ),
    'TRACT'   : (55 , 61 ),
    'BLKGRP'  : (61 , 62 ),
    'BLOCK'   : (62 , 66 ),
    'IUC'     : (66 , 68 ),
    'CONCIT'  : (68 , 73 ),
    'CONCITCC': (73 , 75 ),
    'CONCITSC': (75 , 77 ),
    'AIANHH'  : (77 , 81 ),
    'AIANHHFP': (81 , 86 ),
    'AIANHHCC': (86 , 88 ),
    'AIHHTLI' : (88 , 89 ),
    'AITSCE'  : (89 , 92 ),
    'AITS'    : (92 , 97 ),
    'AITSCC'  : (97 , 99 ),
    'TTRACT'  : (99 , 105),
    'TBLKGRP' : (105, 106),
    'ANRC'    : (106, 111),
    'ANRCCC'  : (111, 113),
    'CBSA'    : (113, 118),
    'CBSASC'  : (118, 120),
    'METDIV'  : (120, 125),
    'CSA'     : (125, 128),
    'NECTA'   : (128, 133),
    'NECTASC' : (133, 135),
    'NECTADIV': (135, 140),
    'CNECTA'  : (140, 143),
    'CBSAPCI' : (143, 144),
    'NECTAPCI': (144, 145),
    'UA'      : (145, 150),
    'UASC'    : (150, 152),
    'UATYPE'  : (152, 153),
    'UR'      : (153, 154),
    'CD'      : (154, 156),
    'SLDU'    : (156, 159),
    'SLDL'    : (159, 162),
    'VTD'     : (162, 168),
    'VTDI'    : (168, 169),
    'RESERVE2': (169, 172),
    'ZCTA5'   : (172, 177),
    'SUBMCD'  : (177, 182),
    'SUBMCDCC': (182, 184),
    'SDELM'   : (184, 189),
    'SDSEC'   : (189, 194),
    'SDUNI'   : (194, 199),
    'AREALAND': (119, 213),
    'AREAWATR': (213, 227),
    'NAME'    : (227, 317),
    'FUNCSTAT': (317, 318),
    'GCUNI'   : (318, 319),
    'POP100'  : (319, 328),
    'HU100'   : (328, 337),
    'INTPTLAT': (337, 348),
    'INTPTLON': (348, 360),
    'LSADC'   : (360, 362),
    'PARTFLAG': (362, 363),
    'RESERVE3': (363, 369),
    'UGA'     : (369, 374),
    'STATENS' : (374, 382),
    'COUNTYNS': (382, 390),
    'COUSUBNS': (390, 398),
    'PLACENS' : (398, 406),
    'CONCITNS': (406, 414),
    'AIANHHNS': (414, 422),
    'AITSNS'  : (422, 430),
    'ANRCNS'  : (430, 438),
    'SUBMCDNS': (438, 446),
    'CD113'   : (446, 448),
    'CD114'   : (448, 450),
    'CD115'   : (450, 452),
    'SLDU2'   : (452, 455),
    'SLDU3'   : (455, 458),
    'SLDU4'   : (458, 461),
    'SLDL2'   : (461, 464),
    'SLDL3'   : (464, 467),
    'SLDL4'   : (467, 470),
    'AIANHHSC': (470, 472),
    'CSASC'   : (472, 476),
    'CNECTASC': (474, 477),
    'MEMI'    : (476, 478),
    'NMEMI'   : (477, 478),
    'PUMA'    : (478, 483),
    'RESERVED': (483, 501),
}

# Our zip downloads have URLs that can be recreated with state/arrevs.
# STATES = {
#     'AL': 'Alabama',
#     'AK': 'Alaska',
#     'AZ': 'Arizona',
#     'AR': 'Arkansas',
#     'CA': 'California',
#     'CO': 'Colorado',
#     'CT': 'Connecticut',
#     'DE': 'Delaware',
#     'DC': 'District_of_Columbia',
#     'FL': 'Florida',
#     'GA': 'Georgia',
#     'HI': 'Hawaii',
#     'ID': 'Idaho',
#     'IL': 'Illinois',
#     'IN': 'Indiana',
#     'IA': 'Iowa',
#     'KS': 'Kansas',
#     'KY': 'Kentucky',
#     'LA': 'Louisiana',
#     'ME': 'Maine',
#     'MD': 'Maryland',
#     'MA': 'Massachusetts',
#     'MI': 'Michigan',
#     'MN': 'Minnesota',
#     'MS': 'Mississippi',
#     'MO': 'Missouri',
#     'MT': 'Montana',
#     'NE': 'Nebraska',
#     'NV': 'Nevada',
#     'NH': 'New_Hampshire',
#     'NJ': 'New_Jersey',
#     'NM': 'New_Mexico',
#     'NY': 'New_York',
#     'NC': 'North_Carolina',
#     'ND': 'North_Dakota',
#     'OH': 'Ohio',
#     'OK': 'Oklahoma',
#     'OR': 'Oregon',
#     'PA': 'Pennsylvania',
#     'PR': 'Puerto_Rico',
#     'RI': 'Rhode_Island',
#     'SC': 'South_Carolina',
#     'SD': 'South_Dakota',
#     'TN': 'Tennessee',
#     'TX': 'Texas',
#     'UT': 'Utah',
#     'VT': 'Vermont',
#     'VA': 'Virginia',
#     'WA': 'Washington',
#     'WV': 'West_Virginia',
#     'WI': 'Wisconsin',
#     'WY': 'Wyoming',    
# }

# This is the template for the URLs
# URL_TEMPLATE_ZIP = 'https://www2.census.gov/census_2010/04-Summary_File_1/{state}/{state_abbrev}2010.sf1.zip'

TEMP_DIR = pathlib.Path(tempfile.gettempdir()) / 'surgeo_temp'
# TEMP_DIR.mkdir(exist_ok=True)

# This creates a URL with each and every state in the STATES dictioanry
# urls = [
#     URL_TEMPLATE_ZIP.format(state_abbrev=code.lower(), state=name)
#     for code, name
#     in STATES.items()
# ]

# def request_data(url, retries):
#     '''Helper that attempts to get file a number of times'''
#     tries = 0
#     while True:
#         try:
#             with urllib.request.urlopen(url) as r:
#                 data = r.read()
#                 return data
#         except Exception:
#             tries += 1
#             if tries >= retries:
#                 raise
#         print('Retrying {url}...'.format(url))

# def dl_file(url, file_path):
#     '''Helper func: downloads zip from URL and stores it in local folder'''
#     # If it exsits do nothing
#     if file_path.exists():
#         print('{} is already present. Processing ...'.format(file_path))
#         pass
#     # Otherwise download file to dir
#     else:
#     # Open request
#         data = request_data(url, 3)
#         file_path.touch()
#         file_path.write_bytes(data)

def make_geo_df(file_path, geo_level="ZCTA"):
    '''Helper func: takes zip and creates a geographic file from data'''
    # Read zip data
    with zipfile.ZipFile(file_path) as zf:
        # Filter out everything except the ZipInfo (geo) for csv we want
        target = zf.filelist[0]
        # Read that CSV into BytesIO object
        geo_data = io.BytesIO(zf.read(target))
        # Read fixed-width file into dataframe
        geo_df = pd.read_fwf(
            geo_data, 
            header=None,
            # Use the GEO MAP but subtract from column start/stop
            colspecs=[
                (tuple_[0] - 1, tuple_[1] - 1)
                for tuple_
                in GEO_MAP_2010.values()
            ],
            dtype=str,
            encoding='latin',
            engine='python'
        )
    # Give names to columns
    geo_df.columns = tuple(GEO_MAP_2010.keys())
    # Filter out all records that are not related to ZCTAs only
    # e.g. get rid of census block data

    # Reference:
    # https://blog.cubitplanning.com/2011/03/census-summary-level-sumlev/
    # https://www2.census.gov/programs-surveys/decennial/rdo/about/2020-census-program/Phase3/SupportMaterials/FrequentSummaryLevels.pdf
    # https://www2.census.gov/programs-surveys/decennial/2010/technical-documentation/complete-tech-docs/summary-file/sf1.pdf

    if geo_level== "ZCTA":
        geo_df = geo_df.loc[geo_df.SUMLEV == '871']
        # Keep the STUSAB (state), LOGRECONO (join key), and ZCTA5 (zip code proxy)
        geo_df = geo_df[['STUSAB', 'LOGRECNO', 'ZCTA5']]#.dropna(subset=['ZCTA5'])
    elif geo_level == 'TRACT':
        geo_df = geo_df.loc[geo_df.SUMLEV == '140']
        geo_df = geo_df[['STUSAB', 'LOGRECNO', 'TRACT', 'STATE', 'COUNTY']]#.dropna(subset=['ZCTA5'])
    elif geo_level == 'BLOCK':
        geo_df = geo_df.loc[geo_df.SUMLEV == '101']
        geo_df = geo_df[['STUSAB', 'LOGRECNO', 'ZCTA5', 'BLKGRP', 'BLOCK']]#.dropna(subset=['ZCTA5'])
    return geo_df

def make_pop_df(file_path ):
    '''Helper func: Takes a zip and creates population df'''
    # Read zip data
    with zipfile.ZipFile(file_path) as zf:
        # Filter out everything except the ZipInfo for csv we want
        # This contains Table P5
        target = zf.filelist[3]
        # Read that CSV into BytesIO object
        pop_data = io.BytesIO(zf.read(target))
        pop_df = pd.read_csv(
            pop_data, 
            header=None,
            dtype=str,
            encoding='latin',
            engine='python'
        )
        # Keep only a subset of columns and renames them
        pop_df = pop_df[[1, 4, 18, 19, 20, 21, 22, 23, 24, 25]]
        pop_df.columns = [
            'STUSAB',
            'LOGRECNO',
            'white',
            'black',
            'native',
            'asian',
            'pi',
            'other',
            'multiple',
            'hispanic',
        ]
        return pop_df

def merge_frames(geo_df, pop_df, geo_level="ZCTA"):
    '''Merges our GEO and POP frames'''
    # Merges common STUSAB and LOGRECNO fields
    merged = geo_df.merge(pop_df)
    # Rename zctq5
    if geo_level=='TRACT':
        merged = merged.rename(columns={'TRACT': 'tract', 'STATE':'state', 'COUNTY':'county'})
        merged = merged.set_index(["state","county","tract"])
        merged = merged.sort_index()
    else:
        merged = merged.rename(columns={'ZCTA5': 'zcta5'})
        # Set index to ZCTA5 and sort
        merged = merged.set_index('zcta5')
        merged = merged.sort_index()
    return merged
    
def create_df(file_path, geo_level="ZCTA"):
    '''Main function to download, join, and clean data for single state'''
    
    # file_name = url.rpartition('/')[2]
    # file_path = temp_dir / file_name 
    # # Download
    # if not file_path.exists():
    #     print(url)
    #     data   = dl_file(url, file_path)
    # else:
    #     print(file_name, " Exists and Cached")
    # Make dfs
    geo_df = make_geo_df(file_path, geo_level)
    pop_df = make_pop_df(file_path)
    # Join DFs, sort, trip, and process
    df     = merge_frames(geo_df, pop_df, geo_level)
    df = df.iloc[:, 2:]
    df = df.astype(np.float64)
    return df

# def main2() -> None:


#     # Join all data into single dataframe and sort index
#     df = pd.concat(data_zcta)
#     df.sort_index(inplace=True)

#     # https://github.com/theonaunheim/surgeo/issues/10
#     # Certain zctas cross state lines and must be added together.
#     df = df.groupby(df.index).apply(sum)

#     # Store column totals
#     totals = df.sum(axis=1)

#     # Store some other race so it can be divvyed up among other groups
#     other = df['other']

#     # Create Asian or Pacific Islander (this is what surname uses)
#     df['api'] = df['asian'] + df['pi']

#     # Drop columns we will no longer use
#     df = df.drop(columns=['other', 'asian', 'pi'])

#     # Now determine what percent of the row each items makes up.
#     percentages = df.divide(totals, axis='rows')

#     # Split up 'other' into the remaining rows based on the percents above
#     apportioned_other = percentages.multiply(other, axis='rows')

#     # Impute 'other' to the remaining groups based on percentage makeup
#     # quasi Iterative proportortional fit / matrix rake over single dimension
#     df += apportioned_other

#     # Reconvert to percentage
#     column_totals = df.sum(axis=0)
#     ratio_by_column = df.divide(column_totals, axis='columns').copy()

#     # Reorder columns
#     ratio_by_column = ratio_by_column[[
#         'white',
#         'black',
#         'api',
#         'native',
#         'multiple',
#         'hispanic'
#     ]]

#     # Reconvert to percentage
#     row_totals = df.sum(axis=1)
#     ratio_by_row = df.divide(row_totals, axis='index').copy()

#     # Reorder columns
#     ratio_by_row = ratio_by_row[[
#         'white',
#         'black',
#         'api',
#         'native',
#         'multiple',
#         'hispanic'
#     ]]

#     data_zcta = []
#     print("Gather the Tract Information")
#     data_tract = [
#         create_df(url, TEMP_DIR, "TRACT")
#         for url
#         in urls
#     ]

#     # REPEAT CALCULATION FOR TRACT LEVEL IF NEEDED

#     # Join all data into single dataframe and sort index
#     df_tract = pd.concat(data_tract)
#     df_tract = df_tract.sort_index()

#     # Store column totals
#     totals_tract = df_tract.sum(axis=1)
#     display(totals_tract.count())


#     other = df_tract['other']
#     df_tract['api'] = df_tract['asian'] + df_tract['pi']
#     df_tract = df_tract.drop(columns=['other', 'asian', 'pi'])
#     percentages = df_tract.divide(totals_tract, axis='rows')
#     apportioned_other = percentages.multiply(other, axis='rows')
#     df_tract += apportioned_other

#     display(df_tract.count())

#     column_totals = df_tract.sum(axis=0)
#     ratio_by_column_tract = df_tract.divide(column_totals, axis='columns').copy()

#     ratio_by_column_tract = ratio_by_column_tract[[
#         'white',
#         'black',
#         'api',
#         'native',
#         'multiple',
#         'hispanic'
#     ]]

#     row_totals = df_tract.sum(axis=1)
#     ratio_by_row_tract = df_tract.divide(row_totals, axis='index').copy()

#     ratio_by_row_tract = ratio_by_row_tract[[
#         'white',
#         'black',
#         'api',
#         'native',
#         'multiple',
#         'hispanic'
#     ]]

#     ratio_by_row_tract.sort_index().head()

#     # WRITE DATA TO MODULE AS CSV

def main():

    from glob import glob

    # print(TEMP_DIR)

    ls_temp = glob(f'{TEMP_DIR}/*.zip')

    df_list = []
    for fp in ls_temp[:4]:
        try: 
            print(f'Processing {fp} ....')
            df_list.append(create_df(fp, geo_level='BLOCK'))
        except :
            print("A problem occurred.")

    # Join all data into single dataframe and sort index
    df = pd.concat(df_list)
    df = df.sort_index()

    # print(df.head())

    # https://github.com/theonaunheim/surgeo/issues/10
    # Certain zctas cross state lines and must be added together.
    df = df.groupby(df.index).apply(sum)

    # Store column totals
    totals = df.sum(axis=1)

    # Store some other race so it can be divvyed up among other groups
    other = df['other']

    # Create Asian or Pacific Islander (this is what surname uses)
    df['api'] = df['asian'] + df['pi']

    # Drop columns we will no longer use
    df = df.drop(columns=['other', 'asian', 'pi'])

    # Now determine what percent of the row each items makes up.
    percentages = df.divide(totals, axis='rows')

    # Split up 'other' into the remaining rows based on the percents above
    apportioned_other = percentages.multiply(other, axis='rows')

    # Impute 'other' to the remaining groups based on percentage makeup
    # quasi Iterative proportortional fit / matrix rake over single dimension
    df += apportioned_other

    # Reconvert to percentage
    column_totals = df.sum(axis=0)
    ratio_by_column = df.divide(column_totals, axis='columns').copy()

    # Reorder columns
    ratio_by_column = ratio_by_column[[
        'white',
        'black',
        'api',
        'native',
        'multiple',
        'hispanic'
    ]]

    # Reconvert to percentage
    row_totals = df.sum(axis=1)
    ratio_by_row = df.divide(row_totals, axis='index').copy()

    # Reorder columns
    ratio_by_row = ratio_by_row[[
        'white',
        'black',
        'api',
        'native',
        'multiple',
        'hispanic'
    ]]

    # WRITE DATA TO MODULE AS CSV

    current_directory = pathlib.Path().cwd()
    project_directory = current_directory.parents[0]
    data_directory    = project_directory / 'surgeo' / 'data'

    # Prob zcta given race
    rbc_path = data_directory / 'prob_zcta_given_race_2010.csv'
    ratio_by_column.to_csv(rbc_path)

    # Prob race given block zcta
    rbr_path = data_directory / 'prob_race_given_zcta_2010.csv'
    ratio_by_row.to_csv(rbr_path)

    # Prob zcta given race tract
    rbc_path = data_directory / 'prob_tract_given_race_2010.csv'
    ratio_by_column_tract.to_csv(rbc_path)

    # Prob race given block tract
    rbr_path = data_directory / 'prob_race_given_tract_2010.csv'
    ratio_by_row_tract.to_csv(rbr_path)

    # CLEAN UP TEMP_DIR

    # Delete files
    for path in TEMP_DIR.rglob('*'):
        path.unlink()

    # Delete dir
    TEMP_DIR.rmdir()

if __name__ == '__main__':
    main()