"""
Gets and orgnaizes data from StatsCan API

Input: StatsCanCounts excel spreadsheet, with list of VectorIds to pull
Output: StatsCan_Output excel spreadsheet

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
    logging.basicConfig(handlers=[RotatingFileHandler('./debugging/getdata_log.log', maxBytes=50000000, backupCount=50)], level=logging.INFO)
    
    #Director will create helper objects, then perform full process
    director= Director()
    director.main()

class Director:
    """Coordintes overall process"""
    def __init__(self):
        self.statscan=statscan_data_manager.StatsCan_Manager()

    def main(self):
        """Imports, sorts, analyzes, and exports data"""
        #Read source file for Vectors to download and extract vector Ids
        source_df = pd.read_excel(sourceFile)
        vectorIds = self.extract_vector_ids(source_df)
        logger.info(f'vector Ids: {vectorIds}')

        #Get list of dictionary version for each data point 
        data_dicts = self.statscan.fetch_data_dicts(vectorIds)

        #Identify and summarize groups of values
        excluded_columns = ['VectorId', 'Data_Value', 'Scaled_Value']
        analyzer = Data_Analyzer(excluded_columns) 
        list_of_grouped_dicts = analyzer.group_and_summarize_data(data_dicts)

        #Adds global group to ensure all data points are included in output
        global_group = Data_group(data_dicts, global_group=True)
        list_of_grouped_dicts.insert(0, global_group)

        #Group data into sheets, organized by Product Id
        export_df = self.statscan.prep_data_for_export(list_of_grouped_dicts)
        
        #Export to excel file
        self.export_to_excel(export_df, outputFile)

    def extract_vector_ids(self, source_df):
        """Organizes all vectors ids into single string for API call"""
        #Initialize empty list of strings
        vectorIds = []
        #Add list of vectorIds within cell as single string to list
        for index, row in source_df.iterrows():
            row_ids = row['Vectors']
            vectorIds.extend(row_ids.split(', '))
        #Return all vectorIds as single string
        return ','.join(vectorIds)

    def export_to_excel(self, dfs, filename):
        """Exports dictionary of pandas dataframes to excel file, where each value is a list of rows that share a product Id, and each sheet contains all values grouped by productId"""
        with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
            for sheet_name, df in dfs.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)  
                worksheet=writer.sheets[sheet_name]
                for i, col in enumerate(df.columns):
                    width=max(df[col].apply(lambda x: len(str(x))).max(), len(col))
                    worksheet.set_column(i, i, width)

class Data_Analyzer:
    """Used to group and summarize data - finds all cross-data variables to perform analysis on, and performs some analysis"""
    def __init__(self, exclude_list):
        """Saves exclude list for sorting groups (NOTE: Value, our DV, is expected to change. We want to allow cross-data (productId) groups as well, if possible)"""
        self.exclude_list = exclude_list

    def group_and_summarize_data(self, data_dicts):
        """Groups data into lists of data who share all but one data point, and averages that group"""
        list_of_groups = []

        # Compare every pair of data points
        for data_point in tqdm(data_dicts, desc="Populating groups for analysis..."):
            for comparison_data_point in data_dicts:
                #Set variables
                data_keys=data_point.keys()
                compare_keys=comparison_data_point.keys()

                #If two points share the same keys, we can compare them
                if data_keys == compare_keys:
                    #Find single key whose value differs between two data points
                    differing_key = self.find_single_difference(data_point, comparison_data_point)
                    
                    #If single differing key found, add to group list
                    if differing_key:
                        #logging.debug(f'Found differing key {differing_key}, adding points to groups')
                        self.add_points_to_groups(list_of_groups, data_point, comparison_data_point, differing_key)
                    
        return list_of_groups

    def add_points_to_groups(self, list_of_groups, data_point, comparison_data_point, differing_key):
        """Adds points to group list. 
           -If group exists with one of the two points and a matching differing key, add missing point
           -If no group exists, create one
           """
        #Initialize match_found, in case neither data point is found
        match_found = False

        #Iterate through existing groups
        for group in list_of_groups:
            #Find if either data point exists in this group
            if data_point in group.group or comparison_data_point in group.group:
                #If the group's differing key is the same as ours, add the data points
                #Note: Filtering for duplicates and updating analysis is done in the group
                if group.differing_key == differing_key:
                    group.add_point(data_point)
                    group.add_point(comparison_data_point)

                    #Change match_found to true
                    match_found = True

        #After completing iteraiton, if we haven't found a match yet, create a new group
        if not match_found:
            logger.debug(f'No match found in list of groups. Creating new group. Data point: {data_point} | Comparison: {comparison_data_point} | Differing key: {differing_key}')
            #Create new list containing both data points
            new_group = Data_group([data_point, comparison_data_point], differing_key)

            #Add group to list
            list_of_groups.append(new_group)

            
    def get_summary_dict(self, group):
        """Finds and returns dictionary in group that has 'Mean (Average)' as one of its values"""
        #Find dictionary with average
        summary_dict = next((d for d in group if 'Mean (Average)' in d.values()), None)

        #Error checking for instances where summary dictionary isn't found in group - this should be impossible, as the summary dictionary is created when the group is.
        if summary_dict is None:
            raise ValueError("Summary dictionary not found in group")
        
        #Return found dictionary
        return summary_dict

    def create_summary_dict(self, group, differing_key):
        """Crate summary dictionary from group of data points, and seed with initial values"""
        #Create summary dict by copying one of the group elements
        summary_dict = group[0].copy()
        #Replace value for differing key with string of "Mean (Average)"
        summary_dict[differing_key]= 'Mean (Average)'
        #Update dictionary values with calculated stats and return
        summary_dict = self.update_summary_dict(summary_dict, group)
        return summary_dict
    
    def update_summary_dict(self, summary_dict, group):
        """Calculates mean values for data value and Scaled_Value (if present)"""
        #If we have values for data values, find mean.
        data_values = [d['Data_Value'] for d in group]
        if summary_dict['Data_Value'] is not None:
            summary_dict['Data_Value'] = mean(data_values)

        #If we have values for Scaled_Values, find mean.
        if 'Scaled_Value' in group[0]:
            scaled_values = [d['Scaled_Value'] for d in group]
            summary_dict['Scaled_Value'] = mean(scaled_values)

        #Return amended dictionary
        return summary_dict

    def find_single_difference(self, data_point, comparison_data_point):
        """Compares two data points to find single differing value"""
        # Immediately return None if we're testing a data point against itself
        #NOTE: REmoved  or data_point['ProductId'] != comparison_data_point['ProductId']: from next line - removing specific varible from generic analyzer
        if data_point == comparison_data_point:
            return None
        
        # Initialize difference counter and differing key tracker
        diff = 0
        differing_key = None
        
        # Iterate through data points, counting differences.
        for key in comparison_data_point:
            #Skip check if key is excluded
            if key not in self.exclude_list:
                #If we find a key with differing values between the two points...
                if data_point[key] != comparison_data_point[key]:
                    # Add one to diff. If this results in more than 1 differences, return None
                    diff += 1
                    differing_key = key
                    #Return when differences equals more than one
                    if diff > 1:
                        return None
                    
        # If diff is 1 after checking all keys, return the differing key
        if diff == 1:
            #logger.debug(f'Diff between {data_point} and {comparison_data_point} = 1! Returning differing key {differing_key}')
            return differing_key
        
        # Otherwise, return None (e.g. if diff = 0)
        else:
            return None
               
class Data_group:
    def __init__(self, initial_points, differing_key=None, global_group=False):
        #Creates group - differing_key and list of dictionaries
        self.group = initial_points
        if differing_key is not None:
            self.differing_key = differing_key
        if global_group == False:
            #Add summary dict to group
            self.add_point(self.get_summary_dict())
            self.update_summary_dict()

    def add_point(self, data_dict):
        """Adds data point to group"""
        #Checks for duplicates
        if data_dict not in self.group:
            #Adds point and updates stats
            self.group.append(data_dict)
            self.update_summary_dict()

    def get_summary_dict(self):
        """Gets summary dictionary. If none exists, creates one"""
        #Create new summary dictionary if none exists
        summary_dict = next((summary for summary in self.group if summary[self.differing_key] == 'Mean (Average)'), None)
        
        if summary_dict == None:
            summary_dict = self.create_summary_dict()        

        return summary_dict

    def create_summary_dict(self):
        """Creates new summary dictionary for data group"""
        #Copy's first item in group for summary dictionary
        summary_dict = self.group[0].copy()

        #Check for cross-productID groups (Note: Shouldn't happen, so nothing big to deal with yet)
        summary_product_id = summary_dict['ProductId']
        if any(member['ProductId']!=summary_product_id for member in self.group):
           differing = next(data_dict for data_dict in self.group if data_dict['ProductId']!=summary_product_id)
           logger.warning(f'Found cross-product Id group, includes summary id {summary_product_id} and differing id {differing}')
        
        #Sets header for summary dictionary
        summary_dict[self.differing_key] = 'Mean (Average)'
        return summary_dict
    
    def update_summary_dict(self):
        """Updates summary dict. """
        summary_dict = self.get_summary_dict()
        """Calculates mean values for data value and Scaled_Value (if present)"""
        #If we have values for data values, find mean.
        #Create list that excludes the summary dictionary
        clean_list = [d for d in self.group if d!=summary_dict]

        data_values = [data_point['Data_Value'] for data_point in clean_list]
        data_ids = [data_point['VectorId'] for data_point in clean_list] #Exemption to specific rule: For debugging only
        data_dates = [data_point['RefPeriod'] for data_point in clean_list] #Exemption to specific rule: For debugging only
        if data_values is not None and len(data_values)>0:
            try:
                summary_dict['Data_Value'] = mean(data_values)
                logger.debug(f'Calculated mean of list {data_values}. Ids of point: {data_ids}. Dates: {data_dates}')
            except:
                summary_dict['Data_Value'] = None
                logger.warning(f'Error calculating mean of list {data_values}. Ids of point: {data_ids}. Dates: {data_dates}')
                
        #If we have values for Scaled_Values, find mean.
        if 'Scaled_Value' in self.group[0]:
            scaled_values = [d['Scaled_Value'] for d in self.group]
            summary_dict['Scaled_Value'] = mean(scaled_values)
        
            



    

#Initializes script and runs
init()