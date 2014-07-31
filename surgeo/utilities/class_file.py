'''This is the main class for using the model in other Python3 programs.
    
   Added to the 'surgeo' namespace.'''

import csv
import io
import itertools
import operator
import os
import sqlite3
import sys

import surgeo

################################################################################

class SurgeoModel(object):
    '''Contains data references and methods for running a BISG model.'''

    def __init__(self):
        # Load entire db to memory for performance DISK DATABSE
        db_path = os.path.join(os.path.expanduser('~'),
                               '.surgeo',
                               'census.db')
        if not os.path.exists(db_path):
            raise SurgeoError('DB does not exist. Run surgeo.data_setup().')
        self.db = sqlite3.connect(db_path)

    def guess_race(self, zcta, surname):
        '''zcta and surname goes in, race string.'''
        result = surgeo.model.model1.run_model(zcta,
                                               surname.upper(),
                                               self.db)
        return result.probable_race
        
    def race_data(self, zcta, surname):
        '''zcta and surname goes in, formatted SurgeoResult comes out.'''
        result = surgeo.model.model1.run_model(zcta,
                                               surname.upper(),
                                               self.db)
        return result
    
    def process_csv(self, 
                    filepath_in,
                    filepath_out,
                    verbose=True):
        '''This takes a csv filepath and creates new csv with race data.'''
        # Create staging area for lines to write
        line_buffer = io.StringIO()
        # Open file, determine if zip and name in header
        with open(filepath_in, 'rU') as input_csv:
            csv_reader = csv.reader(input_csv)
            number_of_columns = len(csv_reader[0])
            for index, item in enumerate(csv_reader[0]):
                if type(item) is str:
                    if 'zip' in item.lower() or 'zcta' in item.lower():
                        zip_index = index
                    if 'last name' in item.lower() or 'surname' in item.lower():
                        surname_index = index
        # zip and name index required. If no index for zip/name, raise error.
        try:
            zip_index
            surname_index
        except NameError:
            raise SurgeoError('.csv row 1 lacks \'zip\' or \'surname\' fields.')
        # Go through all lines, and put amended lines with new data, and stage.
        # Writing to string buffer rather than file.
        csv_writer = csv.writer(line_buffer)
        # Write header row first by splicing together
        chopped_header_row = csv_reader[0][:number_of_columns]
        header_remainer = [ 'surname', 'zip', 'probable_race', 
                            'probable_race_percentage', 'hispanic',
                            'white', 'black', 'asian_or_pi', 'american_indian', 
                            'multiracial' ]
        header_row = [ item for item in 
                       itertools.chain(chopped_header_row, header_remainder) ]                               
        csv_writer.writerow(header_row)
        number_of_rows = len(csv_writer)
        if number_of_rows == 0:
            raise SurgeoError('.csv file appears to be empty.')
        for index, row in enumerate(csv_reader[1:]):
            surname = row[surname_index]
            zcta = row[zip_index]
            # Invoke object's race_data
            if verbose == True:
                percent_complete = round(index/number_of_rows)
                if last_written < percent_complete:
                    sys.stdout.write('\rRead: {}%'.format(percent_complete))
                    last_written = percent_complete
            result = surgeo.model.model1.run_model(zcta,
                                                   surname.upper(),
                                                   self.db)
            result_list = [ result.surname, result.zcta, result.probable_race,
                            result.probable_race_percentage, result.hispanic,
                            result.white, result.black, result.asian_or_pi,
                            result.american_indian, result.multiracial ]
            # Chop row in the event that rows are a different length
            chopped_row = row[:number_of_columns]
            # Splice new row together
            new_row = [item for item in 
                       itertools.chain(chopped_row, results_list)]
            csv_writer.writerow(new_row)
        if verbose == True:
            print('Writing to {}.'.format(filepath_out))
        with open(filepath_out, 'w+') as f:
            f.write(line_buffer.getvalue())
        line_buffer.close()
        if verbose == True:
            print('Complete.')

################################################################################                      
           
class SurgeoResult(object):
    '''Result class containing BISG data.'''
    def __init__(self, 
                 surname, 
                 zcta, 
                 hispanic, 
                 white, 
                 black, 
                 asian_or_pi, 
                 american_indian, 
                 multiracial):
        self.surname = surname
        self.zcta = zcta
        self.zip = zcta
        self.hispanic = hispanic
        self.white = white
        self.black = black
        self.asian_or_pi = asian_or_pi
        self.american_indian = american_indian
        self.multiracial = multiracial
        
    @property
    def probable_race(self):
        rank_dict = { 'Hispanic' : self.hispanic,
                      'White' : self.white,
                      'Black' : self.black,
                      'Asian / Pacific Islander' : self.asian_or_pi,
                      'American Indian / Alaskan Eskimo' : self.american_indian,
                      'Multiracial' : self.multiracial }
        most_probable_race = sorted(rank_dict.items(),
                                    key=operator.itemgetter(1),
                                    reverse=True)[0][0]
        return most_probable_race

    @property
    def probable_race_percentage(self):
        return max(self.hispanic,
                   self.white,
                   self.black,
                   self.asian_or_pi,
                   self.american_indian,
                   self.multiracial)
                   
    @property
    def as_string(self):
        string = ','.join(['probable_race={},'.format(probable_race),
            'probable_race_percent={},'.format(probable_race_percentage),
            'surname={},'.format(self.surname),
            'zip={},'.format(self.zcta),
            'hispanic={},'.format(self.hispanic),
            'white={},'.format(self.white),
            'black={},'.format(self.black),
            'asian={},'.format(self.asian_or_pi),
            'indian={},'.format(self.american_indian),
            'multiracial={},'.format(self.multiracial),
            '\n'])
        return string
       
################################################################################
       
class SurgeoError(Exception):
    '''Custom error class for vanity's sake.'''

    def __init__(self, reason, response=None):
        self.reason = reason
        self.response = response
        Exception.__init__(self, reason)

    def __str__(self):
        return self.reason
