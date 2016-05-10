'''
raw_parser.py

Parser for handling raw text formatted data (e.g. CSV, TSV, etc)

Constructor takes parameters:

file_path: URL or path to data file to be read

column_structure: Specifies column structure of the data file (e.g.
['URL', 'Domain', 'Info'] etc)

discard_columns: Columns to drop from constructed dataframe (i.e.
columns that do not fit required structure or are irrelavent)

comment_delimiter: Character which indicates a commentted line
(All lines prefixed with this character will be ignroed) 

item_delimiter: Character which delimits fields in data file
(e.g. ',', '\t', etc)

skip_num_lines: Ignore the first n lines in the data file

'''

import pandas as pd
import parser_settings as settings

class RAWParser(object):

    def __init__(self):
        self.df_listitems = pd.DataFrame()


    def raw_parse(self, file_path, column_structure, discard_columns, \
                  comment_delimiter, item_delimiter, skip_num_lines):

        print "[RAW] raw_parser: Parsing for ",file_path
        # Read data file into a dataframe, per user-specified params
        try:
            self.df_listitems = pd.read_csv(file_path, \
                                          comment=comment_delimiter, \
                                          names=column_structure, \
                                          sep=item_delimiter, \
                                          skiprows=skip_num_lines)
        except:
            print "[RAWParser] Error: Could not retrieve RAW source file"
            return "Failure"
        # Dataframe is empty, meaing we didn't parse in anything   
        if len(self.df_listitems) == 0:
            print "[RAWParser] Error: Retrieved dataset is empty"
            return "No Data"

        # Drop unneeded columns per discard_columns list
        self.df_listitems.drop(discard_columns, inplace=True, axis=1, errors='ignore')

        # Replace invalid items (i.e. NaN) with a generic message
        self.df_listitems.fillna(value="Unavailable", inplace=True)

        # Add any missing columns from default column structure
        for col in settings.default_column_structure:
            if col not in self.df_listitems.columns.values:
                self.df_listitems[col] = "Unavailable"

        return self.df_listitems
