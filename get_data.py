"""
Gets and orgnaizes data from data collection modules

Input: StatsCanCounts excel spreadsheet, with list of VectorIds to pull
Output: StatsCan_Output excel spreadsheet

Current version only pulls data from Stats Canada. Will include other data sources as available
Formatting notes:
-The data analyzer is relatively source agnostic, though requires a few formatting specifics
--Source values should be in dictionary entries with key "Data_Value"
--Any scaled values should be in dict entries with key "Scaled_Value"

"""

#Import libraries
#Libraries for core behaviour
import pandas as pd
from statistics import mean
import xlsxwriter

#StatsCan data manager handles pulling and organizing data from StatsCan
from scripts import statscan_data_manager

#Libraries for debugging and monitoring
from tqdm import tqdm
import logging
from logging.handlers import RotatingFileHandler
logger = logging.getLogger(__name__)

#Set source and output file locations and names
sourceFile = "data/StatsCanCounts.xlsx"
outputFile = "data/StatsCan_Output.xlsx"

#Initialize objects and process
def init():
    """Initializes script - Creates Director object and begins process"""
    #Initializes logger
    logging.basicConfig(handlers=[RotatingFileHandler('./debugging/getdata_log.log', maxBytes=50000000, backupCount=250)], level=logging.INFO)
    
    #Director will create helper objects, then perform full process
    director= Director()
    director.main()

class Director:
    """Coordintes overall process"""
    def __init__(self):
        
        #Creates manager for data from Statistics Canada
        self.statscan=statscan_data_manager.StatsCan_Manager()

    def main(self):
        
        logger.info("Beginning main loop")
        """Imports, sorts, analyzes, and exports data"""
        #Stats Canada organizes by "Vectors". Vectors are stored in source file
        #Read source file for Vectors to download and extract vector Ids
        source_df = pd.read_excel(sourceFile)
        vectorIds = self.extract_vector_ids(source_df)

        #Get list of dictionary version for each data point for analysis
        data_dicts = self.statscan.fetch_data_dicts(vectorIds)
        logger.debug(f'Data Dicts: {data_dicts}')

        #Prepare data from Statistics Canada for export
        export_df = self.prepare_StatsCan(data_dicts)
        
        #Export to excel file
        self.export_to_excel(export_df, outputFile)

    def extract_vector_ids(self, source_df):
        """Organizes all vectors ids into single string for API call

        Args:
            source_df (dataframe)): source document containing vectors to download

        Returns:
            string: list of vectors, separated by ,
        """        

        #Initialize empty list of strings
        vectorIds = []
        #Add list of vectorIds within cell as single string to list
        for index, row in source_df.iterrows():
            row_ids = row['Vectors']
            vectorIds.extend(row_ids.split(', '))
        #Return all vectorIds as single string
        return ','.join(vectorIds)

    def prepare_StatsCan(self, data_dicts):
        """Groups StatsCan data for summary, performs summaries,
        Organizes data into pandas data frame for export
        returns dataframe

        Args:
            data_dicts (list<dict>): List of dictionaries containing raw data

        Returns:
            dataframe: Pandas DF representation of processed data, including summary statistics
        """        

        excluded_columns = ['VectorId', 'Data_Value', 'Scaled_Value', "Value Per Capita"]
        statsCan_analyzer = Data_Analyzer(data_dicts, excluded_columns)

        #Get list of summary dictionaries from analyzer, adds to list of dictionaries
        summary_dicts = statsCan_analyzer.get_summary_dictionaries()
        data_dicts.extend(summary_dicts)

        #Get Dataframe version of data, sorted by product Id
        export_df = self.convert_list_to_dataframes(data_dicts, 'ProductId', 'Title')

        #Adds list of globally shared keys (and all present values) to dataframe
        export_df['Global variables']=statsCan_analyzer.global_group

        #Returns full dataframe for export        
        return export_df

    def convert_list_to_dataframes(self, data_dicts, sorting_key, name_key):
        """Converts list of data dictionaries into pandas data frames, sorted by given key, labeled as provided

        Args:
            data_dicts (list<dict>): List of raw data dictionaries
            sorting_key (string): Key for data to sort and group by
            name_key (string): Key for data containing name to label sheet with

        Returns:
            dataframe: Pandas data frame with data sorted by sorting key, with each sheet labeled with data from name_key
        """        
        grouped = {}

        for data_point in data_dicts:
            logger.debug(f'Attempting to find data_point[{sorting_key}], {data_point}')
            key = data_point[sorting_key]
            if key not in grouped:
                grouped[key] = []
            grouped[key].append(data_point)

        dfs = {}
        for key, group in grouped.items():
            # Assume all items in the group share the same name_key value
            name = group[0].get(name_key, "Unknown")
            sheet_name = f"{key}-{name}"[:30]
            dfs[sheet_name] = pd.DataFrame(group)

        return dfs

    def export_to_excel(self, dfs, filename):
        """Exports dictionary of pandas dataframes to excel file

        Args:
            dfs (dataframe): Pandas dataframe to export
            filename (_type_): File name to save data into
        """        
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            for sheet_name, df in dfs.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)  
                worksheet=writer.sheets[sheet_name]
                for i, col in enumerate(df.columns):
                    width=max(df[col].apply(lambda x: len(str(x))).max(), len(col))
                    worksheet.set_column(i, i, width)

class Data_Analyzer:
    """Used to group and summarize data - finds all cross-data variables to perform analysis on, and performs some analysis"""
    def __init__(self, data_dicts, exclude_list):
        """Saves exclude list for sorting groups (e.g. unique identifiers - expected to differ between points)"""
        self.exclude_list = exclude_list

        #Groups data into groups that share single dimension
        self.data_groups = self.group_data(data_dicts)
        #Sets global group
        self.global_group = self.get_global_group(data_dicts)

    def group_data(self, data_dicts):
        """Groups data into lists of data who share all but one dimension

        Args:
            data_dicts (list<dict>): List of all raw data points

        Returns:
            list<data_group>: List of groups of data points that can be summarized
        """        

        list_of_groups = []

        # Compare every pair of data points
        for data_point in tqdm(data_dicts, desc="Populating groups for analysis..."):
            for comparison_data_point in data_dicts:
                #If data points share keys, we can compare them
                if data_point.keys() == comparison_data_point.keys():
                    #If points differ by exactly one value, add to list of groups
                    differing_key = self.find_single_difference(data_point, comparison_data_point)
                    if differing_key:
                        self.add_points_to_groups(list_of_groups, data_point, comparison_data_point, differing_key)
        
        #Return assembled list of data_groups
        return list_of_groups

    def get_summary_dictionaries(self):
        """Gets dictionaries that contain summary stats about data points"""
        #Initialize list of summary stats
        summary_list = []        

        #Add dictionaries summarizing averages
        for group in self.data_groups:
            #Add each group's average to summary list
            summary_list.append(group.get_group_average())

        #Returns list of dictionaries containing summary stats
        return summary_list
    
    def get_global_group(self, data_dicts):
        """Find keys that are present across all data, record all possible vlues for each global key

        Args:
            data_dicts (list<dict>): List of all raw data points

        Returns:
            dataframe: Pandas dataframe containing global key/value sets
        """        
        """Gets group of keys that are represented in all data, and includes all key values"""
        shared_keys = set(data_dicts[0].keys())
        shared_keys_values = {}

        for d in data_dicts:
            shared_keys.intersection_update(d.keys())

        for key in shared_keys:
            shared_keys_values[key] = set()

        for d in data_dicts:
            for key in shared_keys:
                shared_keys_values[key].add(d[key])

        # Convert to DataFrame and return
        return pd.DataFrame(dict([(k, pd.Series(list(v))) for k, v in shared_keys_values.items()]))

    def find_single_difference(self, data_point, comparison_data_point):
        """Returns single difference between two data points, unless more or less exist

        Args:
            data_point (data_point): Data point to compare against
            comparison_data_point (data_point): Data point to compare

        Returns:
            string: name of single key where data points differ
            *If the points differ by 0 or >1, return None
        """        
        """"""
        # Bounce if we're testing a data point against itself
        if data_point == comparison_data_point:
            return None
        
        # Initialize difference counter and differing key tracker
        diff = 0
        differing_key = None
        
        # Iterate through data points, counting differences.
        for key in comparison_data_point:
            #If values don't match and key isn't in exclusion list, count differences and set key
            if data_point[key] != comparison_data_point[key] and key not in self.exclude_list:
                diff += 1
                differing_key = key
                #If this results in more than one difference, we return None
                if diff > 1:
                    return None
                    
        # If diff is 1 after checking all keys, return the differing key
        if diff == 1:
            return differing_key
        # Otherwise, return None (e.g. if diff = 0)
        else:
            return None
        
    def add_points_to_groups(self, list_of_groups, data_point, comparison_data_point, differing_key):
        """Adds points to group list. Creates group if needed.

        Args:
            list_of_groups (list<Data_group>): Current list of groups, to be augmented
            data_point, comparison_data_point (data_point): data points to add to group  
            differing_key (string): Key of single differing value between two points 
        """        
        #Initialize match_found, in case neither data point is found
        match_found = False

        #Iterate through existing groups
        for group in list_of_groups:
            #Find if either data point exists in this group
            if data_point in group.group or comparison_data_point in group.group:
                #If the group's differing key is the same as ours, add points
                if group.differing_key == differing_key:
                    group.add_points([data_point, comparison_data_point])

                    #Flag match as found
                    match_found = True

        #If no matches, create a new group
        if not match_found:
            #Create new list containing both data points
            new_group = Data_group([data_point, comparison_data_point], differing_key)

            #Add group to list
            list_of_groups.append(new_group)
               
class Data_group:
    #Defaults to summary stat being Mean (Average). May perform others
    def __init__(self, initial_points, differing_key=None):
        #Creates group - differing_key and list of dictionaries
        self.group = initial_points
        if differing_key is not None:
            self.differing_key = differing_key

    def add_point(self, data_dict):
        """Adds data point to group, unless group already contains point

        Args:
            data_dict (dictionary): Single data point to add to group
        """        
        #Checks for duplicates
        if data_dict not in self.group:
            #Adds point and updates stats
            self.group.append(data_dict)

    def add_points(self, data_dicts):
        """Version of add point that takes list of points rather than single point

        Args:
            data_dicts (list<dict>): List of data points to add to group
        """
        #Iterate through list and add each to group
        for data_dict in data_dicts:
            self.add_point(data_dict)

    def get_group_average(self):
        """Calculates and returns dictionary containing average of group"""
        summary_dict = self.create_summary_dict('Mean (Average)')
        data_values = [data_point['Data_Value'] for data_point in self.group]

        #Calculates mean of group
        if data_values is not None and len(data_values)>0:
            try:
                summary_dict['Data_Value'] = mean(data_values)
                #logger.debug(f'Calculated mean of list {data_values}')
            except:
                summary_dict['Data_Value'] = None
                logger.debug(f'Couldn\'t calculate mean from {data_values}')
                
        #If we have values for Scaled_Values, find mean.
        if 'Scaled_Value' in self.group[0]:
            scaled_values = [d['Scaled_Value'] for d in self.group]
            summary_dict['Scaled_Value'] = mean(scaled_values)

        #Returns dictionary
        return summary_dict
    
    def create_summary_dict(self, summary):
        """Creates new summary dictionary - copies first element and sets header"""
        #Copy's first item in group for summary dictionary
        summary_dict = self.group[0].copy()
        
        #Sets header for summary dictionary
        summary_dict[self.differing_key] = summary
        return summary_dict    

#Initializes script and runs
init()