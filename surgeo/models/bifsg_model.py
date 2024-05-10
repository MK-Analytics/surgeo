"""Module containing Surgeo BIFSG class"""

import pandas as pd

from surgeo.models.base_model import BaseModel
from surgeo.utility.surgeo_exception import SurgeoException

import sys
import pathlib
import tempfile

class BlockLoader:

    def __init__(self): 

        if getattr(sys, 'frozen', False):
            # The application is frozen
            freeze_package = pathlib.Path(sys.executable).parents[0]
            self._package_root = freeze_package / 'Lib' / 'surgeo'
        else:
            # The application is not frozen
            self._package_root = pathlib.Path(__file__).parents[1]

        self._TEMP_DIR = pathlib.Path(tempfile.gettempdir()) / 'surgeo_temp'

        # self._DATA_DIR = f'{self._package_root}/surgeo/data/'
        self._DATA_DIR = f'{self._package_root}/data/'

    def load_fips(self, fips:list[str]):
        
        import pandas as pd

        self.glob_datadir(fips)
        self.load_files()

        return (self.RACE_GIVEN_BLOCK, self.BLOCK_GIVEN_RACE)


    def glob_datadir(self, fips:list[str]) -> None:

        from glob import glob

        path = self._DATA_DIR + '*__*.parquet'

        block_parquet = glob(path)
        # fips_list = [f.split('__')[-1].split('.')[0] for f in block_parquet]
        filepaths = [f for f in block_parquet if f.split('__')[-1].split('.')[0] in fips]
        # list(set([print(f) for f in fips_list if f in list(fips)])

        # for block_fp in block_parquet:
        #     fp = block_fp.split('__')[-1].split('.')[0]
        #     if fp in list(fips):
        #         print(fp)   

        # filepaths = [
        # [print(f) for f in block_parquet if f.split('__')[-1].split('.')[0] in fips]

        # [print(f) for f in filepaths]

        self._rgb, self._bgr = self.sort_stat_tables(filepaths)


        return None
    
    def sort_stat_tables(self, filepaths:list[str]) -> tuple[list[str]]: 

        rgb = [f for f in filepaths if 'race_given_block' in f]

        bgr = [f for f in filepaths if 'block_given_race' in f]

        return (rgb, bgr)
    
    def load_files(self) -> None: 
        
        import pandas as pd

        # [print(i) for i in self._rgb]

        if len(self._rgb) > 0:
            self.RACE_GIVEN_BLOCK = pd.concat([self.load_parquet(f) for f in self._rgb])
            self.BLOCK_GIVEN_RACE = pd.concat([self.load_parquet(f) for f in self._bgr])
        else: 
            raise Exception('No files found')       
        return None


    def load_parquet(self, filepath:str):

        import pyarrow as pa
        import pyarrow.parquet as pq
        
        testload = pq.read_table(filepath)
        return pa.Table.to_pandas(testload)


class BIFSGModel(BaseModel):
    r"""Subclass for running a Bayesian Improved First Name Surname Geocode model.

    This class:

    1. Loads the appropriate first name, surname, and geocode lookup dataframes
       upon instantiation;
    2. Exposes a public get_probabilities() function to compute race
       probabilities based on proxy data (namely first names, surnames and
       ZIP codes); and,
    3. Contains a number of helper functions for cleaning ZCTA/names,
       multiplying probabilities, checking input values, and obtaining
       ZCTA/name data components.

    Notes
    -----
    The surname probability dataframe for this model is identical to that
    used for the SurnameModel (`prob_race_given_surname_2010.csv`);
    the first name probability dataframe for this modelis not the same as that
    used for the FirstNameModel. his model uses the
    `prob_first_name_given_race_harvard.csv` file, which has the percentage of
    a particular race that uses that first name (e.g. 3% of all
    White US citizens have the first name AARON). The FirstNameModel uses the
    `prob_race_given_first_name_harvard.csv` file, which has the race percentages
    for a given first name (e.g. 92% of people with the first name AARON are
    White); the geocode probability dataframe for this model is not the same as
    that used for the GeocodeModel. This model uses the
    `prob_zcta_given_race_2010.csv` file, which has the percentage of
    a particular race that falls within that ZCTA (e.g. .002% of all
    White US citizens live within this ZIP code). The GeocodeModel uses the
    `prob_race_given_zcta_2010.csv` file, which has the race percentages
    for a given ZCTA (e.g. 90% of ZCTA 63144 is White).

    The manner in which the first name data file was created can be found in
    the "fetch_first_names" Jupyter notebook.

    The manner in which the geography data file was created can be found in
    the "fetch_geography" Jupyter notebook.

    This is based of the following general formula from Voicu [#]_.

    | :math:`q(r \mid s,f,g) = \Large \frac{u(r,s,f,g)}{u(1,s,f,g) \, + \, u(2,s,f,g) \, + \, u(3,s,f,g) \, + \, u(4,s,f,g) \, + \, u(5,s,f,g) \, + \, u(6,s,f,g)}`
    |
    | Where:
    | :math:`\hspace{25px} u(r,s,f,g) = p(r \mid s) \times p(g \mid r) \times p(f \mid r)`
    |
    | And where:
    | :math:`\hspace{25px} p(r \mid s)` is the probability of a selected race given surname
    | :math:`\hspace{25px} p(g \mid r)` is the probability of a selected census block of residence given race
    | :math:`\hspace{25px} p(f \mid r)` is the probability of a selected first name given race
    | :math:`\hspace{25px} g` is Census Block
    | :math:`\hspace{25px} f` is First Name
    | :math:`\hspace{25px} s` is Surname
    | :math:`\hspace{25px} r` is Race
    |
    | And where:
    | :math:`\hspace{25px} 1 \text{ is } r =` Hispanic
    | :math:`\hspace{25px} 2 \text{ is } r =` White
    | :math:`\hspace{25px} 3 \text{ is } r =` Black
    | :math:`\hspace{25px} 4 \text{ is } r =` Asian or Pacific Islander
    | :math:`\hspace{25px} 5 \text{ is } r =` American Indian / Alaska Native
    | :math:`\hspace{25px} 6 \text{ is } r =` Multi Racial

    References
    ----------

    .. [#]

        Ioan Voicu "Using First Name Information to Improve Race and Ethnicity
        Classification". Statistics and Public Policy (2018) 5:1, 1-13,
        `<https://www.tandfonline.com/doi/full/10.1080/2330443X.2018.1427012>`_

    """

    GEO_LEVEL_MAP = {
            'ZCTA': 'prob_zcta_given_race_2010.parquet',
            'TRACT': 'prob_tract_given_race_2010.parquet',
            'BLOCK': ''
        }

    def __init__(self, geo_level = 'ZCTA'):
        super().__init__()

        if geo_level in self.GEO_LEVEL_MAP:
            self._GEO_LEVEL = geo_level
        else: 
            raise Exception("geo_level parameter must be 'ZCTA', 'TRACT', 'BLOCK'")

        # These data should be changed to load from pickle, too, for consistency, but they are so small that this is low priority.

        race_given_surname_path = self._package_root / 'data' / 'prob_race_given_surname_2010.parquet'
        fname_given_race = self._package_root / 'data' / 'prob_first_name_given_race_harvard.parquet'

        self._PROB_RACE_GIVEN_SURNAME = self._parquet_to_df(race_given_surname_path)
        self._PROB_FIRST_NAME_GIVEN_RACE = self._parquet_to_df(fname_given_race)

        self.load_loc()

    def load_loc(self):

        if self._GEO_LEVEL in ['ZCTA', 'TRACT']:
            loc_filepath = self._package_root / 'data' / self.GEO_LEVEL_MAP[self._GEO_LEVEL]
            self._PROB_LOC_GIVEN_RACE = self._parquet_to_df(loc_filepath)
        elif self._GEO_LEVEL in ['BLOCK']:
            '''
            Block level data is far larger than the other summary level datasets. Because of this, we will load the data on-the-fly
            when called by the .get_probabilities method. It will check the first 2 digits of all block ids, which contains the state
            FIPS code and then load the appropriate data partitions of the block data.
            '''
            pass 
        else: 
            raise Exception("geo_level must be either 'ZCTA', 'TRACT', or 'BLOCK'")
        
    def _block_load(self, fips:list[str]) -> None:
        state_fips = list(set([i[:2] for i in list(set(fips))]))

        bloader = BlockLoader()
        _, self._PROB_LOC_GIVEN_RACE = bloader.load_fips(state_fips)

        return None

    def get_probabilities(self, first_names, surnames, zctas):
        """Obtain a set of BIFSG probabilities for first_name/surname/ZCTA
        series

        This method first takes the data and checks to see if the data is
        formatted appropriately. It triggers the _get_surname_probs(),
        _get_first_name_probs(), and _get_geocode_probs() helper functions to
        merge the probabilities for the inputs with their looked-up values. It
        then runs the _combined_probs() helper function to actually conduct the
        data calculation and obtain the BIFSG probabilities. It finally runs
        the _adjust_frame() method to concatenate the inputs and outputs in a
        single convenient frame.

        Parameters
        ----------
        first_names : pd.Series
            A series of first names to use for the BIFSG algorithm
        surnames : pd.Series
            A series of surnames to use for the BIFSG algorithm
        zctas : pd.Series
            A series of ZIP/ZCTA codes for the BIFSG algorithm

        Returns
        -------
        pd.DataFrame
            Dataframe of BIFSG probability results

        """

        if self._GEO_LEVEL == 'BLOCK':

            self._block_load(zctas)

        # Check inputs
        self._check_inputs(first_names, surnames, zctas)
        # Get component probabilities
        first_name_probs = self._get_first_name_probs(first_names)
        sur_probs = self._get_surname_probs(surnames)
        geo_probs = self._get_geocode_probs(zctas)
        # Run BIFSG algorithm
        bifsg_probs = self._combined_probs(
            first_name_probs,
            sur_probs,
            geo_probs
        )
        # Combine inputs with results and adjust as necessary
        result = self._adjust_frame(
            first_name_probs,
            sur_probs,
            geo_probs,
            bifsg_probs,
        )
        return result

    def _combined_probs(self,
                        first_name_probs: pd.DataFrame,
                        sur_probs: pd.DataFrame,
                        geo_probs: pd.DataFrame) -> pd.DataFrame:
        """Performs the BIFSG calculation"""
        # Calculate each of the numerators
        bifsg_numer = (
            first_name_probs.iloc[:, 1:] *
            sur_probs.iloc[:, 1:] *
            geo_probs.iloc[:, 1:]
        )
        # Calculate the denominator
        bifsg_denom = bifsg_numer.sum(axis=1)
        # Caluclate the bifsg probabilities (each num / denom)
        bifsg_probs = bifsg_numer.div(bifsg_denom, axis=0)
        return bifsg_probs

    def _adjust_frame(self,
                      first_name_probs: pd.DataFrame,
                      sur_probs: pd.DataFrame,
                      geo_probs: pd.DataFrame,
                      bifsg_probs: pd.DataFrame) -> pd.DataFrame:
        # Build frame from zctas, first names, surnames, and probabilities
        bifsg_data = pd.concat([
            geo_probs['zcta5'].to_frame(),
            first_name_probs
                .rename(columns={'name': 'first_name'})['first_name']
                .to_frame(),
            sur_probs
                .rename(columns={'name': 'surname'})['surname']
                .to_frame(),
            bifsg_probs
        ], axis=1)
        return bifsg_data

    def _check_inputs(self,
                      first_names: pd.Series,
                      surnames: pd.Series,
                      zctas: pd.Series):
        """Check first names, surnames, and ZCTAs and ensure they are the same
        length
        """
        if len(first_names) != len(surnames) or len(surnames) != len(zctas):
            err_string = (
                f'Length mismatch. '
                f'First Name length: {len(first_names)}. '
                f'Surname length: {len(surnames)}. '
                f'ZCTA length: {len(zctas)}.'
            )
            raise SurgeoException(err_string)

    def _get_first_name_probs(self, first_names: pd.Series) -> pd.DataFrame:
        """Normalizes ZCTAs/ZIPs and joins them to their race probs."""
        # Normalize
        normalized_first_names = (
            self._normalize_names(first_names)
                .to_frame()
        )
        # Merge names to dataframe, which gives probs for each name.
        first_name_probs = normalized_first_names.merge(
            self._PROB_FIRST_NAME_GIVEN_RACE,
            left_on='name',
            right_index=True,
            how='left',
        )
        return first_name_probs

    def _get_surname_probs(self, surnames: pd.Series) -> pd.DataFrame:
        """Normalizes names and joins names to their race probabilities."""
        # Normalize names
        normalized_names = (
            self._normalize_names(surnames)
                .to_frame()
        )
        # Merge names to dataframe, which gives probs for each name
        surname_probs = normalized_names.merge(
            self._PROB_RACE_GIVEN_SURNAME,
            left_on='name',
            right_index=True,
            how='left',
        )
        return surname_probs

    def _get_geocode_probs(self, zctas: pd.Series) -> pd.DataFrame:
        """Normalizes ZCTAs/ZIPs and joins them to their race probs."""
        # Normalize
        normalized_zctas = (
            self._normalize_zctas(zctas)
                .to_frame()
        )
        # Merge names to dataframe, which gives probs for each name.
        geocode_probs = normalized_zctas.merge(
            self._PROB_LOC_GIVEN_RACE,
            left_on='zcta5',
            right_index=True,
            how='left',
        )
        return geocode_probs
