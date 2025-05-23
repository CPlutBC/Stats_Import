#Import libraries
import requests
import requests_cache
from datetime import timedelta

#Import and initialize debugging libraries
from tqdm import tqdm
import logging
import urllib3
import pandas as pd
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger=logging.getLogger(__name__)

#Explicitly defines the vectors that contain population info
populationVectorIds = ['1,2,3,4,6,7,8,9,10,11,12,13,14,15']

class StatsCan_Manager:
    """Controls the StatsCan modules to return for data director"""
    def __init__(self):
        #Create API Manager to handle API calls
        self.api = API_Manager()
        #Create data assembler to assemble data
        self.data_assembler = Data_Assembler(self.api, self)

    def fetch_data_dicts(self, vectorIds):
        """Given list of vector ids, returns included data in list of dictionaries

        Args:
            vectorIds (string): list of all vector Ids to download in form of single string, separated by ,

        Returns:
            list<dict>: List of raw data points from statscan vector Ids
        """        
        #Get population vector for per capita reference
        population_vectors=self.api.fetch_vetors(populationVectorIds)
        
        #Save pop data as list of dictionaries
        self.population_dicts=self.data_assembler.assemble_data(population_vectors, False)

        #Get vectors from ids using API manager, and sort into list of dictionaries
        vectors = self.api.fetch_vetors(vectorIds)

        #Assemble vector data into dictionaries and return
        data_dicts = self.data_assembler.assemble_data(vectors)

        return data_dicts

class API_Manager:
    """Controls interactions with API, stores and returns data"""
    def __init__(self):
        #Initialize cached requests session
        cache_backend = requests_cache.backends.FileCache('./data/cache')
        self.session=requests_cache.CachedSession('statscan_cache', expire_after=timedelta(hours=24), backend=cache_backend)
        #Also turn off verification to avoid warnings
        self.session.verify=False

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
        """Fetches Vector data from StatsCan API, based on list of VectorIDs

        Args:
            vectorIds (string): single string with all vector ids, separated by ,

        Returns:
            JSON: JSON representation of data
        """        
        #Construct API call
        url = "https://www150.statcan.gc.ca/t1/wds/rest/getDataFromVectorByReferencePeriodRange"
        params= {
            "vectorIds": vectorIds,
            "startRefPeriod": "2015-01-01",
            "endReferencePeriod": "2025-01-01"}

        #Perform API call, returning result
        return self.statscan_call(url, params)

    def fetch_metadata(self, productId):
        """Fetches metadata for given ProductId

        Args:
            productId (string): string of product id to get metadata from

        Returns:
            JSON: JSON containing metadata for given ProductId
        """        
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
        """Calls StatsCan API with given parameters

        Args:
            url (string): URL for API call
            suffix (str, optional): Suffix contains call-specific data, such as vector or product ids. Defaults to ''.
            call_type (str, optional): Determines whether to use post or get. Defaults to 'get'.

        Returns:
            JSON: JSON response from server
        """        
        #Perform call based on call type
        if call_type=='get':
            response = self.session.get(url, params=suffix, verify=False)
        elif call_type=='post':
            response = self.session.post(url, json=suffix, verify=False)
        
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
    def __init__(self, api, manager):
        """Initializes data assembler, and creates metadata cache to avoid unneccessary repeated api calls"""
        self.api=api
        self.manager=manager
        self.metadata_cache = []
    
    def assemble_data(self, vectors, comparisons=True):
        """Assembles data_point objects from raw vector data

        Args:
            vectors (list<dict>): JSON representation of all vectors
            comparisons (bool, optional): Used to perform per capita and other comparisons, unless loading comparative data (when False). Defaults to True.

        Returns:
            list<dict>: List of data points from vectors
        """        

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

            #logger.info(f'Processing vector {vectorId} with coordinate {coordinate}')

            #Iterate through data points, creating data point helper objects
            for vector_data_point in vector['vectorDataPoint']:
                #Create new data point, and populate information
                data_point = Data_Point(self.api, self.manager, comparisons)
                #Sets data that's shared between data points
                data_point.set_vector_data(productId, coordinate, vectorId)
                #Processes data point into dictionary
                data_point.process_data_point(vector_data_point, metadata, comparisons)

                #Add data point to list
                data_points.append(data_point)

        #Move dictionaries from data points to list
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
    def __init__(self, api, manager, comparisons):
        """Create initial data point with references for other data

        Args:
            api (API_Manager): API manager to fetch metadata calls
            manager (StatsCan_Manager): StatsCan Manager for full porocess
            comparisons (bool): Determines whether we perform comparisons or are setting comparative data
        """
        self.api=api
        self.manager=manager
        self.scale_codes = api.scale_codes
        #Checks to see if the dictionaries exist before pulling them.
        if(comparisons):
            self.population_dicts = manager.population_dicts

    def set_vector_data(self, productId, coordinate, vectorId):
        """Sets information shared between data points within vectors"""
        self.productId=productId
        self.coordinate = coordinate
        self.vectorId = vectorId
        
    def process_data_point(self, data_point, metadata, comparisons):
        """Processes raw data into labeled, scaled, and summarized data point"""
        #Save vector into dictionary
        self.initialize_dictionary(data_point, metadata)
        
        #Processes coordinates, assigning dimension names to keys and coordinate names to values 
        self.process_coordinates()
        #Process Value and per capita measure
        self.process_value(comparisons)

    def initialize_dictionary(self, data_point, metadata):
        """Processes data and metadata into dictionary"""
        #Save passed variables
        self.data_point=data_point
        self.metadata=metadata

        #Assembles data into dictionary
        self.data = {
            'ProductId': self.productId,
            'Title': self.metadata['cubeTitleEn'],
            'RefPeriod': self.data_point['refPer'],
            'VectorId' : self.vectorId
            }

    def process_coordinates(self):
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
                self.data[key] = value
    
    def process_value(self, comparisons):
        """Processes data's value. Renames value keys to avoid conflicts from StatsCan organization, and scales if scalar present"""
        data_value = self.data_point['value']
        self.data['Data_Value']=data_value

        #Process scalar, if present
        scalar_code = self.data_point['scalarFactorCode']

        #If scalar code exists and is a positive integer...
        if scalar_code>0 and data_value is not None:
            #Find scalar's string name in scale codes
            scalar = next((scale_code['scalarFactorDescEn'] for scale_code in self.scale_codes if scale_code['scalarFactorCode'] == scalar_code), None)
            #Assign scalar to dictionary entry
            self.data['Scalar'] = scalar

            #Create entry for Scaled_Value that scales based on the scalar code (NOTE: Scalar codes in StatsCan data correspond to a 10^scalar_code multiplier)
            scaled_value=data_value*(10**scalar_code)
            self.data['Scaled_Value']= scaled_value

        #Calculate comparative values (unless being assembled as part of comparisons)
        if(comparisons):
            #Compare to population data to calculate per capita measure
            self.process_per_capita()

    def process_per_capita(self):
        """Compares values to corresponding population data

        Returns:
            None: Returns none if we cannot perform comparison. Otherwise, update own data
        """
        #Get population data for matching refper and geography
        geo = self.data['Geography']
        year = self.data['RefPeriod']

        #Get matching population vector, if one exists
        pop_vector=next((pv for pv in self.population_dicts if pv['Geography']==geo and pv['RefPeriod']==year), None)

        #Divide data_value by population value to get per capita measure
        if pop_vector:
            data_value = None
            if self.data['Data_Value'] is not None:
                data_value = self.data['Data_Value']
                #logger.debug(f'Processing per capita using raw value {data_value}')
            if 'Scaled_Value' in self.data.keys() and self.data['Scaled_Value'] is not None:
                data_value=self.data['Scaled_Value']
                #logger.debug(f'Processing per capita using Scaled_Value {data_value}')
            if data_value == None:
                #logger.debug(f'Cannot process per capita: No data value or Scaled_Value, skipping')
                return None

            value_per_capita = data_value / pop_vector['Data_Value']
            self.data['Value Per Capita']=value_per_capita
            #logger.info(f' value per capita {value_per_capita}')
            
        else:
            logger.error(f'No matching dict found for geo = {geo} and year={year}')
        
    def get_dimension_and_coordinate_name(self, dimension, coord):
        """Gets dimension and coordinate names

        Args:
            dimension (dict): dictionary containing dimension key:values for names
            coord (integer): Value of current coordinate to be compared with dimension

        Returns:
            string, string: returns strings containing dimension name and coordinate value name 
        """        
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