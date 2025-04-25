import logging
logger=logging.getLogger(__name__)

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