"""Module containing the SurnameModel class."""

import pandas as pd

from .base_model import BaseModel


class SurnameModel(BaseModel):
    """Provides a way to look up race percentages by surname.

    This class uses a get_probabilities() method to provide a simple
    mechanism for obtaining race data. It is created using a simple join
    of a race data table and the surnames that are input.

    Notes
    -----
    The manner in which the surame data file was created can be found in
    the "fetch_surnames" Jupyter notebook.

    The surname probability dataframe for this model is generated from the
    `prob_race_given_surname_2010.csv` file.

    References
    ----------
    1.  United States Census Bureau. Frequently Occurring Surnames from
        the 2010 Census.
        `<https://www.census.gov/topics/population/genealogy/data/2010_surnames.html>`_.
        Last Accessed 2019.12.18.

    """

    def __init__(self):
        super().__init__()
        self._PROB_RACE_GIVEN_SURNAME = self._get_prob_race_given_surname()

    def get_probabilities(self, names: pd.Series) -> pd.DataFrame:
        """Obtain race probabilities for a set of surnames.

        Parameters
        ----------
        names : pd.Series
            names to which to attach race probability data

        Return
        ------
        pd.DataFrame
            Dataframe of race probability results

        """

        # Clean and process names (consistent with Word et al)
        normalized_names = (
            self._normalize_names(names)
                .to_frame()
        )
        # Do a simple join to obtain the names along with provs.
        surname_probs = normalized_names.merge(
            self._PROB_RACE_GIVEN_SURNAME,
            left_on='name',
            right_index=True,
            how='left',
        )
        return surname_probs
