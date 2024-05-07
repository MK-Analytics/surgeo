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

TEMP_DIR = pathlib.Path(tempfile.gettempdir()) / 'surgeo_temp'

def make_geo_df(file_path:str, geo_level="ZCTA") -> pd.DataFrame:
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

    # Reference:
    # https://blog.cubitplanning.com/2011/03/census-summary-level-sumlev/
    # https://www2.census.gov/programs-surveys/decennial/rdo/about/2020-census-program/Phase3/SupportMaterials/FrequentSummaryLevels.pdf
    # https://www2.census.gov/programs-surveys/decennial/2010/technical-documentation/complete-tech-docs/summary-file/sf1.pdf

    if geo_level== "ZCTA":
        geo_df = geo_df.loc[geo_df.SUMLEV == '871']
        # Keep the STUSAB (state), LOGRECONO (join key), and ZCTA5 (zip code proxy)
        geo_df = geo_df[['STUSAB', 'LOGRECNO', 'ZCTA5']]
    elif geo_level == 'TRACT':
        geo_df = geo_df.loc[geo_df.SUMLEV == '140']
        geo_df = geo_df[['STUSAB', 'LOGRECNO', 'STATE', 'COUNTY', 'TRACT']]
    elif geo_level == 'BLOCK':
        geo_df = geo_df.loc[geo_df.SUMLEV == '101']
        geo_df = geo_df[['STUSAB', 'LOGRECNO', 'STATE', 'COUNTY', 'TRACT', 'BLOCK']]
    return geo_df

def make_pop_df(file_path:str) -> pd.DataFrame:
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

def merge_frames(geo_df:pd.DataFrame, pop_df:pd.DataFrame, geo_level="ZCTA") -> pd.DataFrame:
    '''Merges our GEO and POP frames'''
    # Merges common STUSAB and LOGRECNO fields
    merged = geo_df.merge(pop_df)
    # Rename zctq5
    if geo_level=='TRACT':
        merged = merged.rename(columns={'TRACT': 'tract', 'STATE':'state', 'COUNTY':'county'})

        # Collapse state, county, tract to a single string id. Set index to that ID. 
        merged['state'] = merged.state.apply(lambda x: str(x).zfill(2))
        merged['county'] = merged.county.apply(lambda x: str(x).zfill(3))
        merged['tract'] = merged.tract.apply(lambda x: str(x).zfill(6))

        merged['tract'] = merged.apply(lambda x: f'{x.state}{x.county}{x.tract}', axis = 1)

        merged = merged.set_index('tract')
        merged.drop(columns=['state', 'county'], inplace=True)
        merged = merged.sort_index()
    elif geo_level=='BLOCK': 
        merged = merged.rename(columns={'BLOCK': 'block', 'STATE':'state', 'COUNTY':'county', 'TRACT': 'tract'})

        # Collapse state, county, tract, and block to a single string ID. Set index to that ID.
        merged['state'] = merged.state.apply(lambda x: str(x).zfill(2))
        merged['county'] = merged.county.apply(lambda x: str(x).zfill(3))
        merged['tract'] = merged.tract.apply(lambda x: str(x).zfill(6))
        merged['block'] = merged.block.apply(lambda x: str(x).zfill(4))

        merged['block'] = merged.apply(lambda x: f'{x.state}{x.county}{x.tract}{x.block}', axis = 1)

        merged = merged.set_index('block')
        merged.drop(columns=['state', 'county', 'tract'], inplace=True)
        merged = merged.sort_index()
    else:
        merged = merged.rename(columns={'ZCTA5': 'zcta5'})
        # Set index to ZCTA5 and sort
        merged = merged.set_index('zcta5')
        merged = merged.sort_index()
    return merged
    
def create_df(file_path:str, geo_level="ZCTA") -> pd.DataFrame:
    '''Main function to download, join, and clean data for single state'''
    
    geo_df = make_geo_df(file_path, geo_level)
    pop_df = make_pop_df(file_path)
    # Join DFs, sort, trip, and process
    df     = merge_frames(geo_df, pop_df, geo_level)
    df = df.iloc[:, 2:]
    df = df.astype(np.float64)
    return df

def run_zcta(filepath_list:list[str]) -> None: 
    '''
    Runs the zipcode summary level of calculations.
    '''

    from tqdm import tqdm

    print("Processing data for zipcode-level summary:")
    
    data_zcta = []
    for fp in tqdm(filepath_list):
        try: 
            # print(f'Processing {fp} ....')
            data_zcta.append(create_df(fp, geo_level='ZCTA'))
        except :
            print("A problem occurred.")

    # Join all data into single dataframe and sort index
    df = pd.concat(data_zcta)
    df = df.sort_index()

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

    write_files(ratio_by_column, ratio_by_row, 'prob_zcta_given_race_2010.parquet', 'prob_race_given_zcta_2010.parquet', mode = 'parquet')


def run_tract(filepath_list:list[str]) -> None: 
    '''
    Runs the tract-level summary of calculations.
    '''

    from tqdm import tqdm

    print("Processing data for tract-level summary")

    
    data_tract = []
    for fp in tqdm(filepath_list):
        try: 
            # print(f'Processing {fp} ....')
            data_tract.append(create_df(fp, geo_level='TRACT'))
        except :
            print("A problem occurred.")

    # Join all data into single dataframe and sort index
    df_tract = pd.concat(data_tract)
    df_tract = df_tract.sort_index()

    # Store column totals
    totals_tract = df_tract.sum(axis=1)
    # display(totals_tract.count())


    other = df_tract['other']
    df_tract['api'] = df_tract['asian'] + df_tract['pi']
    df_tract = df_tract.drop(columns=['other', 'asian', 'pi'])
    percentages = df_tract.divide(totals_tract, axis='rows')
    apportioned_other = percentages.multiply(other, axis='rows')
    df_tract += apportioned_other

    # display(df_tract.count())

    column_totals = df_tract.sum(axis=0)
    ratio_by_column_tract = df_tract.divide(column_totals, axis='columns').copy()

    ratio_by_column_tract = ratio_by_column_tract[[
        'white',
        'black',
        'api',
        'native',
        'multiple',
        'hispanic'
    ]]

    row_totals = df_tract.sum(axis=1)
    ratio_by_row_tract = df_tract.divide(row_totals, axis='index').copy()

    ratio_by_row_tract = ratio_by_row_tract[[
        'white',
        'black',
        'api',
        'native',
        'multiple',
        'hispanic'
    ]]

    ratio_by_row_tract.sort_index().head()

    write_files(ratio_by_column_tract, ratio_by_row_tract, 'prob_tract_given_race_2010.parquet', 'prob_race_given_tract_2010.parquet', mode='parquet')

    return None

def run_block(filepath_list:list[str]) -> None: 
    '''
    Runs the summary calculations for the census block level data.
    '''

    fips_codes_by_abbreviation = {
        "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06",
        "CO": "08", "CT": "09", "DE": "10", "FL": "12", "GA": "13",
        "HI": "15", "ID": "16", "IL": "17", "IN": "18", "IA": "19",
        "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24",
        "MA": "25", "MI": "26", "MN": "27", "MS": "28", "MO": "29",
        "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
        "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
        "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45",
        "SD": "46", "TN": "47", "TX": "48", "UT": "49", "VT": "50",
        "VA": "51", "WA": "53", "WV": "54", "WI": "55", "WY": "56",
        "AS": "60", "GU": "66", "MP": "69", "PR": "72", "VI": "78",
        "UM": "74", "DC": "11"
    }

    from tqdm import tqdm


    print("Processing data for block-level summary")

    # data_block = []
    for fp in tqdm(filepath_list):
        try: 
            # print(f'Processing {fp} ....')

            state_abbrev2 = fp.split('/')[-1][:2].upper()
            state_fips = fips_codes_by_abbreviation[state_abbrev2]

            # print(state_abbrev2, state_fips)

            data_block = create_df(fp, geo_level='BLOCK')
            data_block = data_block.sort_index()

            # Store column totals
            totals_tract = data_block.sum(axis=1)

            other = data_block['other']
            data_block['api'] = data_block['asian'] + data_block['pi']
            data_block = data_block.drop(columns=['other', 'asian', 'pi'])
            percentages = data_block.divide(totals_tract, axis='rows')
            apportioned_other = percentages.multiply(other, axis='rows')
            data_block += apportioned_other

            column_totals = data_block.sum(axis=0)
            ratio_by_column_block = data_block.divide(column_totals, axis='columns').copy()

            ratio_by_column_block = ratio_by_column_block[[
                'white',
                'black',
                'api',
                'native',
                'multiple',
                'hispanic'
            ]]

            row_totals = data_block.sum(axis=1)
            ratio_by_row_block = data_block.divide(row_totals, axis='index').copy()

            ratio_by_row_block = ratio_by_row_block[[
                'white',
                'black',
                'api',
                'native',
                'multiple',
                'hispanic'
            ]]

            write_files(ratio_by_column_block, ratio_by_row_block, 
                        f'prob_block_given_race_2010__{state_fips}.parquet', 
                        f'prob_race_given_block_2010__{state_fips}.parquet', 
                        mode='parquet')

        except Exception as e:
            print(e)
            # print("A problem occurred.")

    return None

def run_block_old(filepath_list:list[str]) -> None: 
    '''
    Runs the summary calculations for the census block level data.
    '''

    from tqdm import tqdm
    
    print("Processing data for block-level summary")

    data_block = []
    for fp in tqdm(filepath_list):
        try: 
            # print(f'Processing {fp} ....')
            data_block.append(create_df(fp, geo_level='BLOCK'))
        except Exception as e:
            print(e)
            # print("A problem occurred.")

    # Join all data into single dataframe and sort index
    data_block = pd.concat(data_block)
    data_block = data_block.sort_index()

    # Store column totals
    totals_tract = data_block.sum(axis=1)

    other = data_block['other']
    data_block['api'] = data_block['asian'] + data_block['pi']
    data_block = data_block.drop(columns=['other', 'asian', 'pi'])
    percentages = data_block.divide(totals_tract, axis='rows')
    apportioned_other = percentages.multiply(other, axis='rows')
    data_block += apportioned_other

    column_totals = data_block.sum(axis=0)
    ratio_by_column_block = data_block.divide(column_totals, axis='columns').copy()

    ratio_by_column_block = ratio_by_column_block[[
        'white',
        'black',
        'api',
        'native',
        'multiple',
        'hispanic'
    ]]

    row_totals = data_block.sum(axis=1)
    ratio_by_row_block = data_block.divide(row_totals, axis='index').copy()

    ratio_by_row_block = ratio_by_row_block[[
        'white',
        'black',
        'api',
        'native',
        'multiple',
        'hispanic'
    ]]

    write_files(ratio_by_column_block, ratio_by_row_block, 'prob_block_given_race_2010.parquet', 'prob_race_given_block_2010.parquet', mode='parquet')

    return None

def write_files(ratio_by_column:pd.DataFrame, ratio_by_row:pd.DataFrame, rbc_filename:str, rbr_filename:str, mode='parquet') -> None:
    current_directory = pathlib.Path().cwd()
    project_directory = current_directory.parents[0]
    data_directory    = project_directory / 'surgeo' / 'data'

    # For efficiency of data storage and performance, drop all data for which there are no known probabilities.
    ratio_by_column.dropna(subset=['white', 'black', 'api', 'multiple', 'hispanic'], inplace=True)
    ratio_by_row.dropna(subset=['white', 'black', 'api', 'multiple', 'hispanic'], inplace=True)

    # Prob location given race
    rbc_path = data_directory / rbc_filename

    # Prob race given location
    rbr_path = data_directory / rbr_filename
    
    if mode == 'csv':
        ratio_by_column.to_csv(rbc_path)
        ratio_by_row.to_csv(rbr_path)
    elif mode == 'pickle':
        import pickle
        with open(rbc_path, 'wb') as f: 
            pickle.dump(ratio_by_column, f)
        with open(rbr_path, 'wb') as f: 
            pickle.dump(ratio_by_row, f)
    elif mode == 'parquet': 
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(ratio_by_column)
        pq.write_table(table, rbc_path)

        table = pa.Table.from_pandas(ratio_by_row)
        pq.write_table(table, rbr_path)

    else: 
        raise Exception("Mode is not recognized. Choose 'csv' or 'pickle'")

    return None

def cleanup_temp_files() -> None:

    # Delete files
    for path in TEMP_DIR.rglob('*'):
        path.unlink()

    # Delete dir
    TEMP_DIR.rmdir()

    return None

def main() -> None:

    from glob import glob

    TEMP_DIR = pathlib.Path(tempfile.gettempdir()) / 'surgeo_temp'

    ls_temp = glob(f'{TEMP_DIR}/*.zip')

    # run_zcta(ls_temp)
    # run_tract(ls_temp)
    run_block(ls_temp)

    # Remove the raw zipped data files
    # cleanup_temp_files()
    

if __name__ == '__main__':
    main()