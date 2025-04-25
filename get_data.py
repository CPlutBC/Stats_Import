"""
Gets and orgnaizes data from StatsCan API

Input: StatsCanCounts excel spreadsheet, with list of VectorIds to pull
Output: StatsCan_Output excel spreadsheet

"""

#Import libraries
#Libraries for core behaviour
import requests
import pandas as pd
from statistics import mean
from scripts import statscan_data_manager
#Libraries for debugging and monitoring
from tqdm import tqdm
import logging
logger = logging.getLogger(__name__)

#Set source and output file locations and names
sourceFile = "data/StatsCanCounts.xlsx"
outputFile = "data/StatsCan_Output.xlsx"

#Initialize objects and process
def init():
    """Initializes script - Creates Director object and begins process"""
    #Initializes logger
    logging.basicConfig(filename='getdata_log.log', level=logging.ERROR)
    
    #Director will create helper objects, then perform full process
    director= Director()
    director.main()

class Director:
    """Coordintes overall process"""
    def __init__(self):
        #Create API Manager to handle API calls
        self.api = statscan_data_manager.API_Manager()
        #Create data assembler to assemble data
        self.data_assembler = Data_Assembler(self.api)

    def main(self):
        """Imports, sorts, analyzes, and exports data"""
        
        #Read source file for Vectors to download and extract vector Ids
        source_df = pd.read_excel(sourceFile)
        vectorIds = self.extract_vector_ids(source_df)

        #Get vectors from ids using API manager, and sort into list of dictionaries
        vectors = self.api.fetch_vetors(vectorIds)
        data_dicts = self.data_assembler.assemble_data(vectors)

        #Use analyzer to identify groups for analysis
        #Note: Analysis is performed during grouping step
        excluded_columns = ['VectorId', 'Value', "Data_Value", "Scaled Value"]
        analyzer = Data_Analyzer(excluded_columns) 
        list_of_grouped_dicts = analyzer.group_and_summarize_data(data_dicts)

        #Group data into sheets, organized by Product Id
        export_df = self.prep_data_for_export(list_of_grouped_dicts)
        
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

    def prep_data_for_export(self, list_of_grouped_dicts):
        """Takes grouped data, and re-groups into data frames for export to excel, organized by Product Id.
        De-duplicates grouped data, so that each data frame contains one row per dat point"""
        #Initialize product id to sheet dictionary, to track included productIds
        product_id_to_sheet = {}

        #Iterate through groups of data.
        for grouped_data in list_of_grouped_dicts:
            #Iterate through grouped data points
            for dict in grouped_data:
                #Get product Id of data point
                productId = dict['ProductId']
                #If product id is not present in the product_id_to_sheet dictionary keys, add it
                if productId not in product_id_to_sheet:
                    product_id_to_sheet[productId] = []
                
                #If the current data point dictionary is not in the corresponding dictionary value, add it
                if dict not in product_id_to_sheet[productId]:
                    product_id_to_sheet[productId].append(dict)

        # Convert each sheet to a pandas DataFrame and store in dictionary, organized by product id
        dfs = {f'Product_{product_id}': pd.DataFrame(sheet) for product_id, sheet in product_id_to_sheet.items()}
        return dfs

    def export_to_excel(self, dfs, filename):
        """Exports dictionary of pandas dataframes to excel file, where each value is a list of rows that share a product Id, and each sheet contains all values grouped by productId"""
        with pd.ExcelWriter(filename) as writer:
            for sheet_name, df in dfs.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)   


class API_Manager:
    """Controls interactions with API, stores and returns data"""
    def __init__(self):
        #Get code sets, to allow for fetching definitions
        self.code_sets=self.get_code_sets()
        #Get scalar code definitions
        self.scale_codes = self.code_sets['object']['scalar']

    def get_code_sets(self):
        """Gets code sets - text descriptions of numerical codes"""
        #Prepare API call
        url = "https://www150.statcan.gc.ca/t1/wds/rest/getCodeSets"
        
        #Perform API call, returning result
        return self.statscan_call(url)

    def fetch_vetors(self, vectorIds):
        """Fetches Vector data from StatsCan API, based on list of VectorIDs"""
        #Construct API call
        url = "https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorByReferencePeriodRange"
        params= {
            "vectorIds": vectorIds,
            "startRefPeriod": "2015-01-01",
            "endReferencePeriod": "2025-01-01"}

        #Perform API call, returning result
        return self.statscan_call(url, params)

    def fetch_metadata(self, productId):
        """Fetches metadata for given ProductId"""
        #Get metadata from Stats Can 
        url = "https://www150.statcan.gc.ca/t1/wds/rest/getCubeMetadata"
        raw_data = self.statscan_call(url, [{"productId": productId}], 'post')

        #If we have 
        if raw_data:
            metadata = raw_data[0]['object']
            return metadata
        else:
            logger.error(f"No data from api from product id {productId}")

    def statscan_call(self, url, suffix = '', call_type = 'get'):
        """Calls StatsCan API. Takes suffix for get/post params/json, and call type for get/post
        Defaults to blank suffix and get call"""
        #Perform call based on call type
        if call_type=='get':
            response = requests.get(url, params=suffix)
        elif call_type=='post':
            response = requests.post(url, json=suffix)
        
        #Returns call info if successful, otherwise, returns response status code
        if response.status_code == 200:
            #If successful, return data
            data = response.json()
            return data     
        else:
            #Error catch - if problem in response, print and return None
            logger.error(f"Error: {response.status_code}")
            return None
        


class Data_Assembler:
    def __init__(self, api):
        """Initializes data assembler, and creates metadata cache to avoid unneccessary repeated api calls"""
        self.api=api
        self.metadata_cache = []

    def assemble_data_points(self, vectors):
        """Assembles data from vector data and metadata"""
        #Initialize empty list to store data
        data_points = []

        #Assemble list of data points, matched with metadata information
        #Iterate through vectors
        for raw_vector in tqdm(vectors, desc="Assembling data points"):
            vector = raw_vector['object']
            #Get productId and metadata
            productId= vector['productId']
            metadata = self.get_metadata(productId)
            #Get other vector-wide info
            coordinate = vector['coordinate']
            vectorId = vector['vectorId']
            logger.info(f'Processing vector {vectorId} with coordinate {coordinate}')

            #Iterate through data points, creating data point objects
            for vector_data_point in vector['vectorDataPoint']:
                #Create new data point, and populate information
                data_point = Data_Point(self.api.scale_codes)
                data_point.set_vector_data(productId, coordinate, vectorId)
                data_point.process_data_point(vector_data_point, metadata)
                #Add data point to list
                data_points.append(data_point)

        return data_points
    
    def assemble_data(self, vectors):
        "Assembles data points and then outputs into dictionary"
        #Assemble data points into objects
        data_points = self.assemble_data_points(vectors)
        #Move data points into dictionary
        data_dicts = [data_point.data for data_point in data_points]

        #Return dictionary version of data points
        return data_dicts

    def get_metadata(self, productId):
        """Gets Cube metadata from metadata cache, or from API if it doesn't exist yet in cache"""
        #Find metadata matching productId in cache, returning none if not found
        metadata = next((data for data in self.metadata_cache if int(data['productId']) == int(productId)), None)
        #Return metadata if found in cache
        if metadata:
            return metadata
        #Otherwise, fetch metadata through API and save to cache
        else:
            metadata = self.api.fetch_metadata(productId)
            self.metadata_cache.append(metadata)
            return metadata

    
class Data_Point:
    """Helper class for assembling data point"""
    def __init__(self, scale_codes):
        """Saves scalar codes for assigning string value"""
        self.scale_codes = scale_codes

    def set_vector_data(self, productId, coordinate, vectorId):
        """Sets information shared between data points within vectors"""
        self.productId=productId
        self.coordinate = coordinate
        self.vectorId = vectorId

    def process_data_point(self, data_point, metadata):
        """Processes data and metadata into dictionary"""
        #Save passed variables
        self.data_point=data_point
        self.metadata=metadata

        #Use all data to create dictionary representation of data.
        self.data = self.data_to_dict()

    def data_to_dict(self):
        """Combines metadata and data to fill dictionary with key:dimension and value:value"""
        #Initiailize new dictionary with product id, data cube title, and reference period 
        dict = {'ProductId': self.productId,
                 'Title': self.metadata['cubeTitleEn'],
                 'RefPeriod': self.data_point['refPer'],
                 'VectorId' : self.vectorId}
        
        #Processes coordinates, assigning dimension names to keys and coordinate names to values 
        self.process_coordinates(dict)

        #Save value in dictionary under key "data_value", scales and adds key:value pairs for scale code definition and scaled value if scalar code present
        #(NOTE: Some StatsCan dimensions have key "Value", which do not contain the value. Renaming avoids conflicts)
        self.process_value(dict)

        #Return filled dictionary
        return dict

    def process_value(self, dict):
        """Processes data's value. Renames value keys to avoid conflicts from StatsCan organization, and scales if scalar present"""
        data_value = self.data_point['value']
        dict['Data_Value']=data_value
        
        #Process scalar, if present
        scalar_code = self.data_point['scalarFactorCode']
        #If scalar code exists and is a positive integer...
        if scalar_code>0 and data_value is not None:
            #Create entry for where key: Scalar and value is string definition of scale code whose scalar code matches the data point's scalar code
            dict['Scalar'] = next((scale_code['scalarFactorDescEn'] for scale_code in self.scale_codes if scale_code['scalarFactorCode'] == scalar_code), None)
            #Create entry for scaled value that scales based on the scalar code (NOTE: Scalar codes in StatsCan data correspond to a 10^scalar_code multiplier)
            dict['Scaled Value']= data_value * (10**scalar_code)

    def process_coordinates(self, dict):
        """Enumerates through coordintes, creating dictionary entries where key: dimension name and value: coordinate (member) name"""
        #Split coordinates into list
        coordinates = self.coordinate.split('.')

        #Enumerate through coordinates list to get names and values for dimensions
        for i, coord in enumerate(coordinates):
            #Ensure JSON contains info on dimension (NOTE: Coordinates are always given in 10 dimensions, JSON only has dimensions represented in data)
            if i < len(self.metadata['dimension']):
                #Find dimension and coordinate name in metadata, for current coordinate
                key, value = self.get_dimension_and_coordinate_name(i, coord)
                #Save coordinate as dictionary entry, with key of dimension name
                dict[key] = value
    
    def get_dimension_and_coordinate_name(self, dimension, coord):
        """Gets dimension and coordinate (member) name """
        #Find given dimension in metadata, save name
        dimension = self.metadata['dimension'][dimension]
        dimension_name = dimension['dimensionNameEn']

        #Get list of dimension members (NOTE: In StatsCan data, "Member" refers to the possible values for coordinates, e.g. "Geography" members are 1: "All Provinces", 2: "Alberta", etc.)
        members=dimension['member']
        #Get member whose ID value matches the given coordinate, save name
        member = next((member for member in members if int(member['memberId']) == int(coord)), None) 
        member_name = member["memberNameEn"]
        
        #Check if member_name includes the dimension name, strip dimension name if so (NOTE: Artefact of StatsCan data - some data has dimension name repeated in member names)
        if ':' in member_name:
            dimension_name, name = member_name.split(': ', 1)
            member_name = name.capitalize()
        if dimension_name == "Value":
            dimension_name = "Value_desc"

        #Return dimension and coordinate (member) name
        return dimension_name, member_name

class Data_Analyzer:
    """Used to group and summarize data - finds all cross-data variables to perform analysis on, and performs some analysis"""
    def __init__(self, exclude_list):
        """Saves exclude list for sorting groups (NOTE: Value, our DV, is expected to change. We want to allow cross-data (productId) groups as well, if possible)"""
        self.exclude_list = exclude_list

    def group_and_summarize_data(self, data_dicts):
        """Groups data into lists of data who share all but one data point, and averages that group"""
        list_of_grouped_dicts = []

        # Compare every pair of data points
        for data_point in tqdm(data_dicts, desc="Populating groups for analysis..."):
            for comparison_data_point in data_dicts:
                #Skip unless keys are identical
                if data_point.keys() == comparison_data_point.keys():
                    #Find single key that differs between two data points
                    differing_key = self.compare_data_point(data_point, comparison_data_point)
                    #Skip unless single key found 
                    if differing_key:
                        #Add points to groups. If group exists with matching differing key, add missing data point. Otherwise, create new group
                        self.add_points_to_groups(list_of_grouped_dicts, data_point, comparison_data_point, differing_key)
                    
        return list_of_grouped_dicts

    def add_points_to_groups(self, list_of_grouped_dicts, data_point, comparison_data_point, differing_key):
        """Adds points to group list. 
           -If group exists with one of the two points and a matching differing key, add missing point
           -If no group exists, create one"""
        #Initialize match_found, in case neither data point is found
        match_found = False

        #Iterate through existing groups
        for grouped_data in list_of_grouped_dicts:
            #Find if either data point exists in group
            if any(dp in grouped_data for dp in [data_point, comparison_data_point]):
                # Confirm that the value that differs between data_point and comparison_data_point is also the value that differs with other members of grouped_data
                if all(self.compare_data_point(dp, comparison_data_point) == differing_key for dp in grouped_data):
                    #Add whichever data point isn't currently in the group
                    if data_point not in grouped_data:
                        grouped_data.append(data_point)
                    if comparison_data_point not in grouped_data:
                        grouped_data.append(comparison_data_point)
                    #Change match_found to true
                    match_found = True

                    #Find summary dictionary in amended group
                    summary_dict = self.get_summary_dict(grouped_data)
                    #Update summary stats from amended group
                    summary_dict = self.update_summary_dict(summary_dict, grouped_data)
                    break
        #If no match found in groups
        if not match_found:
            #Create new list containing both data points
            new_group = [data_point, comparison_data_point]
            #Add summary stats dictionary to group
            new_group.append(self.create_summary_dict(new_group, differing_key))
            # Add created list of dictionaries to list_of_grouped_dicts
            list_of_grouped_dicts.append(new_group)

            
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
        """Craete summary dictionary from group of data points, and seed with initial values"""
        #Create summary dict by copying one of the group elements
        #TODO: Add check for when productID does differ?
        summary_dict = group[0].copy()
        #Replace value for differing key with string of "Mean (Average)"
        summary_dict[differing_key]= 'Mean (Average)'
        #Update dictionary values with calculated stats and return
        summary_dict = self.update_summary_dict(summary_dict, group)
        return summary_dict
    
    def update_summary_dict(self, summary_dict, group):
        """Calculates mean values for data value and scaled value (if present)"""
        #If we have values for data values, find mean.
        data_values = [d['Data_Value'] for d in group]
        if summary_dict['Data_Value'] is not None:
            summary_dict['Data_Value'] = mean(data_values)

        #If we have values for scaled values, find mean.
        if 'Scaled Value' in group[0]:
            scaled_values = [d['Scaled Value'] for d in group]
            summary_dict['Scaled Value'] = mean(scaled_values)

        #Return amended dictionary
        return summary_dict

    def compare_data_point(self, data_point, comparison_data_point):
        """Compares two data points to find single differing value"""
        # Immediately return None if we're testing a data point against itself
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
                    if diff > 1:
                        return None
                    
        # If diff is 1 after checking all keys, return the differing key
        if diff == 1:
            return differing_key
        
        # Otherwise, return None (e.g. if diff = 0)
        else:
            return None
               

#Initializes script and runs
init()