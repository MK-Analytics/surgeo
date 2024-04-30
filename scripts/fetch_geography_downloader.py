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
STATES = {
    'AL': 'Alabama',
    'AK': 'Alaska',
    'AZ': 'Arizona',
    'AR': 'Arkansas',
    'CA': 'California',
    'CO': 'Colorado',
    'CT': 'Connecticut',
    'DE': 'Delaware',
    'DC': 'District_of_Columbia',
    'FL': 'Florida',
    'GA': 'Georgia',
    'HI': 'Hawaii',
    'ID': 'Idaho',
    'IL': 'Illinois',
    'IN': 'Indiana',
    'IA': 'Iowa',
    'KS': 'Kansas',
    'KY': 'Kentucky',
    'LA': 'Louisiana',
    'ME': 'Maine',
    'MD': 'Maryland',
    'MA': 'Massachusetts',
    'MI': 'Michigan',
    'MN': 'Minnesota',
    'MS': 'Mississippi',
    'MO': 'Missouri',
    'MT': 'Montana',
    'NE': 'Nebraska',
    'NV': 'Nevada',
    'NH': 'New_Hampshire',
    'NJ': 'New_Jersey',
    'NM': 'New_Mexico',
    'NY': 'New_York',
    'NC': 'North_Carolina',
    'ND': 'North_Dakota',
    'OH': 'Ohio',
    'OK': 'Oklahoma',
    'OR': 'Oregon',
    'PA': 'Pennsylvania',
    'PR': 'Puerto_Rico',
    'RI': 'Rhode_Island',
    'SC': 'South_Carolina',
    'SD': 'South_Dakota',
    'TN': 'Tennessee',
    'TX': 'Texas',
    'UT': 'Utah',
    'VT': 'Vermont',
    'VA': 'Virginia',
    'WA': 'Washington',
    'WV': 'West_Virginia',
    'WI': 'Wisconsin',
    'WY': 'Wyoming',    
}

# This is the template for the URLs
URL_TEMPLATE_ZIP = 'https://www2.census.gov/census_2010/04-Summary_File_1/{state}/{state_abbrev}2010.sf1.zip'

TEMP_DIR = pathlib.Path(tempfile.gettempdir()) / 'surgeo_temp'
TEMP_DIR.mkdir(exist_ok=True)

# This creates a URL with each and every state in the STATES dictioanry
urls = [
    URL_TEMPLATE_ZIP.format(state_abbrev=code.lower(), state=name)
    for code, name
    in STATES.items()
]

def request_data(url:str, retries:int):
    '''Helper that attempts to get file a number of times'''
    tries = 0
    while True:
        try:
            with urllib.request.urlopen(url) as r:
                data = r.read()
                return data
        except Exception:
            tries += 1
            if tries >= retries:
                raise
        print(f'Retrying {url}')

def dl_file(url:str, file_path:str) -> None:
    '''Helper func: downloads zip from URL and stores it in local folder'''
    # If it exsits do nothing
    if file_path.exists():
        print('{} is already present. Processing ...'.format(file_path))
        pass
    # Otherwise download file to dir
    else:
    # Open request
        data = request_data(url, 3)
        file_path.touch()
        file_path.write_bytes(data)

    return None
    
def create_df(url:str, temp_dir, geo_level="ZCTA") -> None:
    '''Main function to download, join, and clean data for single state'''
    
    file_name = url.rpartition('/')[2]
    file_path = temp_dir / file_name 
    # Download
    if not file_path.exists():
        print('.... downloaded')
        data   = dl_file(url, file_path)
    else:
        print(file_name, " Exists and Cached")

def main() -> None:

    print('Starting download ....')

    df_list = []
    bad_urls = []
    for url in urls:
        try:
            print(f'.... {url}')
            df_list.append(create_df(url, TEMP_DIR))
        except:
            bad_urls.append(url)
            
    print('.... Download complete')

    print(f'Files not loaded: {len(bad_urls)}')
    print(''*60)
    [print(url) for url in bad_urls]

if __name__ == '__main__':
    main()