import abc
import datetime
import json
import os
import time
import uuid
import xml.etree.ElementTree as ET
from multiprocessing import Process
from operator import itemgetter

import math
import psycopg2.extras

from AmazonSelling.tools import datetime_floor, call_sql, get_request, record_timestamps, write_to_file, \
    get_credentials, con_postgres


class WmRoutine:
    """
    Periodically runs searches to update SQL database with item data. Makes sure to stay within API call volume limits.
    Keeps a log file or database in order to know where to begin if an interruption occurs.
    Keeps taxonomy up-to-date.
    """
    
    def __init__(self):
        self.maxDailyCalls = 4750
        self.subcatsList = []        

    def routine(self, **kwargs):
        
        for key, value in kwargs.items():
            if key == 'triggs':
                triggs = value
        
        Taxo().update_taxos()
        self.taxo_to_mem()
 
        getAllProc = Process(target=self.get_all, args=(triggs,))
        getAllProc.start()
         
        # Continuously do mark_wm_dups() every <gap> seconds while WmRoutine.routine() is running
        markwmdupsProc = Process()
        # gap = 25
        time.sleep(10)
        while True:
            # prev = time.time()
            if not markwmdupsProc.is_alive():
                print("Starting mark_wm_dups")
                markwmdupsProc = Process(target=mark_wm_dups)
                markwmdupsProc.start()
                markwmdupsProc.join()
                print("mark_wm_dups has finished")
            # time.sleep(max(prev + gap - time.time(), 0))
                # Break up the long wait between mark_wm_dups into chunks to check for ending conditions
                for _ in range(10):                    
                    time.sleep(60)
                    if not getAllProc.is_alive():
                        break
                if not getAllProc.is_alive():
                    break
         
        mark_wm_dups()  # One last time
        getAllProc.join()
        print("Walmart routine is done!")

    def taxo_to_mem(self):
        """
        Reads WmTaxo_Static from SQL and puts it into a list of lists
        """
        
        con = con_postgres()

        sqlTxt = '''SELECT full_id, last_searched--, include
                    FROM "WmTaxo_Updated"
                    WHERE active IS TRUE
                    AND (include = 1 OR include IS Null)
                    AND (success NOT IN ('4003', 'totalResults_value_is_0') OR last_searched IS Null OR EXTRACT(EPOCH FROM (localtimestamp - last_searched)/86400) > 30)
                    ORDER BY last_searched ASC'''
        self.subcatsList = call_sql(con, sqlTxt, [], 'executeReturn', dictCur=True)
        
        if con:
            con.close()
    
    def get_all(self, triggs=None):
        """
        Goes through all the subcats and retrieves all the products (up to 1000) for each one. Writes to SQL
        """

        con = con_postgres()
        overCallLimit = False
        
        for i in range(0, len(self.subcatsList)):  # Loops through all the subcats
            
            # Checks WmQueryLog to see if too many calls to the Walmart API have been made in the last 24 hours
            sqlTxt = '''SELECT COALESCE(SUM(num_queries), 0)
                        FROM "WmQueryLog"
                        WHERE EXTRACT(EPOCH FROM (localtimestamp - "timestamp")/86400) < 1'''  # Counts # of queries in last 24 hours
            hng = call_sql(con, sqlTxt, [], 'executeReturn')[0][0]
            if hng >= self.maxDailyCalls:
                print("Over daily call limit for the Walmart API! ({})".format(hng))
                if triggs:
                    for _, value in triggs['send'].items():
                        value.send('Over Walmart API daily call limit of {}'.format(self.maxDailyCalls))
                        value.close()
                overCallLimit = True
                break
            
            # Retrieve all the products for the subcat from the Walmart API
            SearchSubcat(self.subcatsList[i]["full_id"]).get_all_for_subcat()
        
        if con:
            con.close()
        
        if not overCallLimit:
            print('Exhausted all queriable subcategories with the Walmart API!')
            if triggs:
                for _, value in triggs['send'].items():
                    value.send('Over Walmart API daily call limit of {}'.format(self.maxDailyCalls))
                    value.close()


class SearchSubcat:
    """
    Used to search for all items in a given sub-category. This class records how many total items result from a search
    query (which is just * for the sub-category) and calculates how many searches are needed to retrieve all items. It
    keeps track of how many searches have already been performed.
    """
    
    def __init__(self, subCat):
        self.subCat = subCat
        self.totalResults = None
        self.status = {'successes': 0, 'failures': 0, 'errors': []}
        self.internetConnection = True

    def get_all_for_subcat(self):
        """
        Gets all items' data for a subcategory and writes it to SQL
        """
        
        numSearches = 0
            
        while True:
            thisSearch = SearchJSON(startIndex=(numSearches * 25) + 1, subCat=self.subCat)
            
            thisSearch.api_search()
            totalRslts, errFlag, errVal = thisSearch.prep_data()
            
            if totalRslts and not self.totalResults:
                self.totalResults = totalRslts
            self.process_errors(errFlag, errVal)
            
            if self.internetConnection:
                numSearches += 1
                
                if errFlag or not self.totalResults:
                    break
                else:
                    searchData = thisSearch.parse_data()
                    thisSearch.write_to_sql(searchData)
                    searchData = None
                    
                if numSearches * 25 >= self.totalResults or numSearches * 25 >= 1000:
                    break
        
        con = con_postgres()
        
        if not self.status["errors"]:  # No errors were returned by the API
            # All successful: success. All failed: failed. Some of each: partial. Something else: <error>.
            if self.status["failures"] > 0:
                if self.status["successes"] > 0:
                    success = "partial"
                else:
                    success = "failed"
            elif self.status["successes"] > 0:
                success = "success"
            else:
                success = "none"
        else:
            success = ','.join(map(str, self.status["errors"]))  # Combine all the error codes into a string
        
        update_wm_query_log(num=numSearches)
        
        # Update WmTaxo_Updated
        sqlTxt = '''UPDATE "WmTaxo_Updated"
                    SET success = %s
                    WHERE full_id = %s'''
        theData = [success, self.subCat]
        call_sql(con, sqlTxt, theData, "executeNoReturn")
        
        if self.totalResults > -1:
            sqlTxt = '''UPDATE "WmTaxo_Updated"
                        SET last_searched = %s, num_items = %s
                        WHERE full_id = %s'''
            theData = [datetime_floor(1.0/60), self.totalResults, self.subCat]
            call_sql(con, sqlTxt, theData, "executeNoReturn")
        
        if con:
            con.close()
        
    def process_errors(self, errFlag, errVal):
        
        self.internetConnection = True
        self.status["successes"] += 1
        
        if errFlag:
            errVal = str(errVal)
            self.status["successes"] -= 1
            self.status["failures"] += 1
            if errVal not in self.status["errors"]:
                self.status["errors"].append(errVal)
            
            if errVal in ('4003', 'totalResults_value_is_0'):  # subcategory not found / invalid category id
                self.totalResults = 0  # This will let the last_searched value in WmTaxo_Updated get updated, so we don't keep checking this subcat every time.
            else:
                self.totalResults = -1
                
            if errVal in ('no_data_returned_from_api', 'Elementtree_init_failure'):
                self.internetConnection = False
                
            if errVal in ('no_data_returned_from_api', 'Elementtree_init_failure', '5000', '503', '504'):
                time.sleep(300)
                

class SearchQuery:
    """
    Runs the Search operation with a query, and writes to SQL, until either there are no more results, or 1000
    items has been reached
    
    Takes: qry
    """
    
    def __init__(self, qry):
        self.qry = qry
        self.totalResults = None
        self.status = {'successes': 0, 'failures': 0, 'errors': []}
        self.internetConnection = True
        
    def get_all_for_query(self):
        """
        Gets all items' data for a query and writes it to SQL.
        Not actually doing anything with the errors yet.
        """
        
        numSearches = 0
            
        while True:
            thisSearch = SearchJSON(startIndex=(numSearches * 25) + 1, theQry=self.qry)
            
            thisSearch.api_search()
            totalRslts, errFlag, errVal = thisSearch.prep_data()
            
            if totalRslts and not self.totalResults:
                self.totalResults = totalRslts
            self.process_errors(errFlag, errVal)
            
            if self.internetConnection:
                numSearches += 1
                
                if errFlag or not self.totalResults:
                    break
                else:
                    searchData = thisSearch.parse_data()
                    thisSearch.write_to_sql(searchData)
                    searchData = None
                    
                if numSearches * 25 >= self.totalResults or numSearches * 25 >= 1000:
                    break
        
        update_wm_query_log(num=numSearches)
        
    def process_errors(self, errFlag, errVal):
        
        self.internetConnection = True
        self.status["successes"] += 1
        
        if errFlag:
            errVal = str(errVal)
            self.status["successes"] -= 1
            self.status["failures"] += 1
            if errVal not in self.status["errors"]:
                self.status["errors"].append(errVal)
            
            if errVal in ('4003', 'totalResults_value_is_0'):  # subcategory not found / invalid category id
                self.totalResults = 0  # This will let the last_searched value in WmTaxo_Updated get updated, so we don't keep checking this subcat every time.
            else:
                self.totalResults = -1
                
            if errVal in ('no_data_returned_from_api', 'Elementtree_init_failure'):
                self.internetConnection = False
                
            if errVal in ('no_data_returned_from_api', 'Elementtree_init_failure', '5000', '503', '504'):
                time.sleep(300)

    
class Search:

    __metaclass__ = abc.ABCMeta

    ext = None  # This will be overwritten by the subclasses
    
    def __init__(self, startIndex, subCat=None, theQry='*'):
        self.subCat = subCat
        self.startIndex = startIndex
        self.qry = theQry
        self.resultTxt = None
        self.errChecked = None
        self.apiKey = get_credentials({'WalmartAPI': 'apiKey'})
    
    def api_search(self):
        """
        Runs a Search API query as defined by <self.subCat>, <self.startIndex>, <self.qry>, <self.ext>
        
        Requires: <self.subCat>, <self.startIndex>, self.qry, <self.ext>
        Produces: <self.resultTxt>
        """
        
        if self.subCat:
            subCatStr = '&categoryId={}'.format(self.subCat)
        else:
            subCatStr = ''
        
        urlStr = ('http://api.walmartlabs.com/v1/search?apiKey={0}{1}&query={2}&numItems=25'
                  '&start={3}&sort=bestseller&responseGroup=full&facet=on'
                  '&facet.range=price:[0 TO 60]&facet.filter=retailer:Walmart.com'
                  '&facet.filter=pickup_and_delivery:Ship to Home&format={4}'
                  .format(self.apiKey, subCatStr, self.qry, self.startIndex, self.ext))
        
#         print(urlStr)
        
        resultLib = get_request(urlStr, 12, 3)
         
        if resultLib['result'] is not None:
            print("[Path: {}, Query: '{}'], start at {}: {} {}".format(self.subCat, self.qry, self.startIndex, resultLib['numTries'], "try" if resultLib['numTries'] == 1 else "tries"))
            self.resultTxt = resultLib['result'].text            
     
            with open(os.path.join(os.path.dirname(__file__), 'DataFiles/Search.{}'.format(self.ext)), 'w', encoding="utf-8") as f:
                f.write(self.resultTxt)                
                
#         with open('H:\\Arbitrage\\Walmart\\DataFiles\\Search.json') as q:
#             self.resultTxt = q.read()
    
    def prep_data(self):        
        """
        Processes <self.resultTxt>, and extracts and prints any errors that might occur along the way
        
        Requires: self.resultTxt, self.ext
        Produces: self.errChecked
        Returns: totalResults, errFlag, errVal
        """
        
        self.errChecked = xml_json_err_check(self.resultTxt, self.ext)
        
        # See if the API returned an error
        if self.errChecked['isErr']:
            errFlag = True
            errVal = self.errChecked['datums']
            totalResults = None
        else:
            totalResults, errFlag, errVal = self.err_check()
            
        if totalResults == 0 and not errFlag:
            errFlag = True
            errVal = 'totalResults_value_is_0'
            
        if errFlag:
            print("Error code <{}> returned from a search request for subcat '{}', query '{}'".format(errVal, self.subCat, self.qry))
            if self.resultTxt:
                write_to_file("{}.{}".format(errVal, self.ext), self.resultTxt, dirrr='DataFiles', absPath=False)
            
        return totalResults, errFlag, errVal
        
    @abc.abstractmethod
    def err_check(self):
        """
        Isolates errors that occur while processing the XML/JSON Search result
        
        Requires: self.errChecked
        Produces: self.root
        Returns: totalResults, errFlag, errVal
        """
        pass
    
    @abc.abstractmethod   
    def parse_data(self):
        """
        Extracts the product data from the XML/JSON Search result
        
        Requires: self.root (for SearchXML), self.theJson (for SearchJSON)
        Returns: theData
        """
        pass

    def check_and_fix_upc(self, upc, wmId):
        """
        Returns a 12-digit upc if <upc> is a numeric string, else returns None
        
        Parameters: upc, wmId
        """
        
        try:
            k = upc.isdigit()
        except AttributeError:
            print('AttributeError for upc.isdigit() for Walmart item # <{}>'.format(wmId))
            return None
        
        if k:
            if not isinstance(upc, str):
                upc = str(upc)

            if len(upc) > 12:
                print('Walmart item # <{}> has an invalid upc: <{}>. The upc has too many digits.'.format(wmId, upc))
                return None
            else:
                return str(upc.zfill(12))  # Pad zeros to the left up to 12 digits
        else:
            print('Walmart item # <{}> has an invalid upc: <{}>'.format(wmId, upc))
            return None
    
    def write_to_sql(self, theData):
        """
        Writes Search API product data to SQL
        
        Parameters: theData
        """
        
        if theData:
            sqlTxt = '''INSERT INTO "Prod_Wm" (fetched, wm_id, name, price, upc, model, brand, in_stock, avail_online, free_ship, clearance)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT ("wm_id") DO UPDATE
                        SET fetched = %s, name = %s, price = %s, upc = %s, model = %s, brand = %s, in_stock = %s,
                        avail_online = %s, free_ship = %s, clearance = %s'''
            theData.sort(key=itemgetter(1))
            con = con_postgres()
            call_sql(con, sqlTxt, theData, "executeBatch")
            
            if self.subCat:  # Need to update with path
                theData2 = [[self.subCat, g[1]] for g in theData]  # Get wm_id along with the subCat
                theData2.sort(key=itemgetter(1))
                call_sql(con, 'UPDATE "Prod_Wm" SET path = %s WHERE wm_id = %s', theData2, "executeBatch")
            
            con.close()
            
            
class SearchXML(Search):
    
    root = None
    ext = 'xml'
    
    def err_check(self):
        
        # totalResults, errFlag, errVal = (None, None, None)
        
        try:
            tree = ET.ElementTree(self.errChecked['datums'])
        except Exception as errFlag:  # Likely lost internet connection <--- This might be a relic from when "self.queryResult.encode('utf-8')" was included in the above line
            errVal = 'Elementtree_init_failure'
        
        else:
            try:
                self.root = tree.getroot()
            except Exception as errFlag:
                errVal = 'getroot_failure'
            
            else:
                try:
                    totalResults = int(self.root.find("totalResults").text)
                except AttributeError as errFlag:  # "totalResults" was not found in self.root
                    errVal = 'totalResults_tag_not_found'
                else:
                    errFlag, errVal = (None, None)
                    
        return totalResults, errFlag, errVal
    
    def parse_data(self):
        
        theData = []
        if self.root:
            ts = datetime_floor(1.0/60)
            tagsTupl = ("itemId", "name", "salePrice", "upc", "modelNumber", "brandName", "stock", "availableOnline", "freeShippingOver35Dollars", "clearance")
            
            for w in self.root.findall(".//items/item"):
                values = dict.fromkeys(tagsTupl)  # Initializes a dictionary with keys from tagsTupl, and all values as None
                
                for y in list(w):  # "list" is an elementree function
                    if y.tag in tagsTupl:
                        values[y.tag] = y.text
                
                # Only add this item if it has a numeric upc
                values['upc'] = self.check_and_fix_upc(values['upc'], values['itemId'])
                if values['upc']:
                    theData.append((ts, values["itemId"], values["name"], values["salePrice"], values["upc"], values["modelNumber"], values["brandName"],
                                    values["stock"], values["availableOnline"], values["freeShippingOver35Dollars"], values["clearance"],
                                    # ON CONFLICT DO UPDATE values start here
                                    ts, values["name"], values["salePrice"], values["upc"], values["modelNumber"], values["brandName"],
                                    values["stock"], values["availableOnline"], values["freeShippingOver35Dollars"], values["clearance"]))
                    
        return theData
    
    
class SearchJSON(Search):
    
    theJson = None
    ext = 'json'
    
    def err_check(self):
        
        # totalResults, errFlag, errVal = (None, None, None)
        totalResults = None
        
        try:
            self.theJson = self.errChecked['datums']
        except Exception as e:
            errVal, errFlag = 'JSON_error', e
        
        else:
            try:
                totalResults = int(self.theJson['totalResults'])
            except KeyError as e:  # "totalResults" was not found in self.theJson
                errVal, errFlag = 'totalResults_tag_not_found', e
            else:
                errVal, errFlag = None, None
                    
        return totalResults, errFlag, errVal
    
    def parse_data(self):
        
        theData = []        
        if self.theJson['items']:
            ts = datetime_floor(1.0/60)
            preConflictTags = ("itemId", "name", "salePrice", "upc", "modelNumber", "brandName", "stock", "availableOnline", "freeShippingOver35Dollars", "clearance")
            postConflictTags = ("name", "salePrice", "upc", "modelNumber", "brandName", "stock", "availableOnline", "freeShippingOver35Dollars", "clearance")
            jsonItems = self.theJson['items']
            
            for i in jsonItems:
                
                # Only add this item if it has a numeric upc
                if 'upc' in i and 'itemId' in i:
                    i['upc'] = self.check_and_fix_upc(i['upc'], i['itemId'])
                    
                    if i['upc']:
                        theData.append((ts,) + tuple(i[tag] if tag in i else None for tag in preConflictTags) + 
                                       (ts,) + tuple(i[tag] if tag in i else None for tag in postConflictTags))
        
        return theData
    
    
class Paginated:
    """
    For using the Paginated Products API
    https://developer.walmartlabs.com/docs/read/Paginated_Products_API
    """
    
    def __init__(self):
        self.apiKey = get_credentials({'WalmartAPI': 'apiKey'})
    
    def api_paginated(self):
        """
        Runs a Paginated Products API query as defined by <self.cat>
        
        Requires: <self.cat>
        Produces: <self.resultTxt>
        """
        pass
        
    
class Lookup:
    """
    For using the Product Lookup API
    https://developer.walmartlabs.com/docs/read/Home
    """
    
    def __init__(self):
        self.apiKey = get_credentials({'WalmartAPI': 'apiKey'})
        self.err = {'isErr': False, 'datums': None}
        
    def lookup_batch(self, wmIdsTupl):
        """
        Fetches and writes to SQL the lookup data for a potentially large number of wm_ids
        <wmIdsTupl> is a tuple of wm_ids
        """
        
        # Get 20 wm_ids at a time from wmIdsTupl
        for i in range(0, math.ceil(len(wmIdsTupl) / 20)):
            queryResult = self.lookup(wmIdsTupl[20 * i:min(len(wmIdsTupl), 20 * (i + 1))])
            
            theJson = xml_json_err_check(queryResult, 'json')  # This will also write the error to file, if there is an error
            if not theJson['isErr']:
                self.json_to_sql(theJson['datums'])
    
    def lookup(self, smallWmIdsTupl):
        # Returns a list of dicts of the parsed API data for the given wm_ids
        # <smallWmIdsTupl> is a tuple of wm_ids with no more than 20 elements
        
        urlStr = 'http://api.walmartlabs.com/v1/items?ids={}&apiKey={}&format=json'.format(','.join(str(w) for w in smallWmIdsTupl), self.apiKey)
        resultLib = get_request(urlStr, 15, 3)
         
        if resultLib['result'] is not None:
            print('Product Lookup - {} wm_ids: {} {}'.format(len(smallWmIdsTupl), resultLib['numTries'], 'try' if resultLib['numTries'] == 1 else 'tries'))
            queryResult = resultLib['result'].text
            
            write_to_file('product_lookup.json', queryResult, dirrr='DataFiles', absPath=False)
         
        else:  
            print("Request result returned None for the following input for walmartclasses.Lookup.lookup:")
            print(smallWmIdsTupl)
            
        return queryResult

    def json_to_sql(self, theJson):
        # Takes a raw Product Lookup JSON string and writes it to SQL
        
        jsonItems = theJson['items']
        theData = []
        preConflictTags = ("itemId", "name", "salePrice", "upc", "modelNumber", "brandName", "stock", "availableOnline", "freeShippingOver35Dollars", "clearance")
        postConflictTags = ("name", "salePrice", "upc", "modelNumber", "brandName", "stock", "availableOnline", "freeShippingOver35Dollars", "clearance")
        ts = datetime_floor(1.0/60)
        
        for i in jsonItems:            
            theData.append((ts,) + tuple(i[tag] if tag in i else None for tag in preConflictTags) + 
                           (ts,) + tuple(i[tag] if tag in i else None for tag in postConflictTags))

        if theData:
            sqlTxt = '''INSERT INTO "Prod_Wm" (fetched, wm_id, name, price, upc, model, brand, in_stock, avail_online, free_ship, clearance)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT ("wm_id") DO UPDATE
                        SET fetched = %s, name = %s, price = %s, upc = %s, model = %s, brand = %s, in_stock = %s,
                        avail_online = %s, free_ship = %s, clearance = %s'''
            theData.sort(key=itemgetter(1))
            con = con_postgres()
            call_sql(con, sqlTxt, theData, "executeBatch")
            con.close()
            

class Taxo:
    """
    This is for getting the taxonomy from the Walmart API and writing to SQL
    This also has intertwine_taxos
    """
    
    def __init__(self):
        self.taxoxml = None
        self.apiKey = get_credentials({'WalmartAPI': 'apiKey'})

    def update_taxos(self):
        success = self.req_taxo()
        if success:
            self.append_taxo()
            self.intertwine_taxos()
            print("Walmart taxonomy has been fetched")

    def req_taxo(self):
        """
        Query the Walmart API for the taxonomy and write it to a file
        """
        
        urlStr = 'http://api.walmartlabs.com/v1/taxonomy?apiKey={}&format=xml'.format(self.apiKey)
        resultLib = get_request(urlStr, 12, 3)
        try:
            self.taxoxml = resultLib['result'].text
        except AttributeError as p:
            print('AttributeError in Taxo.req_taxo: {}'.format(p))
            return False
        
        with open(os.path.join(os.path.dirname(__file__), 'TaxonomyStuff/Taxonomy.xml'), 'w', encoding="utf-8") as f:
            f.write(self.taxoxml)
            
        return True
    
    def append_taxo(self):
        """
        Write taxonomy to SQL.
        The UUID is used to tell which subcats are no longer active, since they won't have the most recent update_uuid.
        """
        
        root = ET.ElementTree(ET.fromstring(self.taxoxml.encode('utf-8'))).getroot()
        
        updateuuid = psycopg2.extras.UUID_adapter(uuid.uuid4())
        
        theData = []
        con = con_postgres()
        for dept in root.findall("category"):
            
            for cats in dept.findall("children"):        
                for cat in cats.findall("category"):
                    
                    for subCats in cat.findall("children"):
                        for subCat in subCats.findall("category"):
                            full_id = subCat.find("id").text
                            dept_id = int(dept.find("id").text.split('_')[0])
                            dept_name = dept.find("name").text
                            cat_id = int(cat.find("id").text.split('_')[1])
                            cat_name = cat.find("name").text
                            subCat_id = int(subCat.find("id").text.split('_')[2])
                            subCat_name = subCat.find("name").text
                            
                            theData.append([full_id, dept_id, dept_name, cat_id, cat_name, subCat_id, subCat_name, updateuuid, datetime.date.today(),
                                            # ON CONFLICT DO UPDATE values start here
                                            dept_name, cat_name, subCat_name, updateuuid])
                            
        sqlTxt = '''INSERT INTO "WmTaxo_Updated" (full_id, dept_id, dept_name, cat_id, cat_name, subcat_id, subcat_name, update_uuid, birthdate)
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (full_id) DO UPDATE
                    SET dept_name = %s, cat_name = %s, subcat_name = %s, update_uuid = %s'''
        call_sql(con, sqlTxt, theData, "executeBatch")
        
        # Set "active" column in WmTaxo_Updated to True when that row's uuid matches the new one
        sqlTxt = '''UPDATE "WmTaxo_Updated"
                    SET active = True
                    WHERE update_uuid = %s;
                    UPDATE "WmTaxo_Updated"
                    SET active = False
                    WHERE update_uuid != %s;'''
        call_sql(con, sqlTxt, [updateuuid, updateuuid], "executeNoReturn")
        
        if con:
            con.close()

    def intertwine_taxos(self):
        """
        Share some columns between WmTaxo_Static and WmTaxo_Updated
        """
                
        con = con_postgres()
        
        # Copy "include" column from WmTaxo_Static to WmTaxo_Updated
#         sqlTxt = '''UPDATE "WmTaxo_Updated" t2
#                     SET include = t1.include
#                     FROM "WmTaxo_Static" t1
#                     WHERE t2.full_id = t1.full_id
#                     AND t2.include IS DISTINCT FROM t1.include'''
#         call_sql(con, sqlTxt, [], "executeNoReturn")
        
        # Copy new subcats over to WmTaxo_Static from WmTaxo_Updated. On conflict, update "active" column
        sqlTxt = '''INSERT INTO "WmTaxo_Static" (full_id, dept_id, dept_name, cat_id, cat_name, subCat_id, subCat_name, active)
                    SELECT full_id, dept_id, dept_name, cat_id, cat_name, subCat_id, subCat_name, active
                    FROM "WmTaxo_Updated"
                    ON CONFLICT (full_id) DO UPDATE
                    SET active = excluded.active'''
        call_sql(con, sqlTxt, [], "executeNoReturn")


def wm_db_query(items):
    """
    Retrieve data for the Walmart products from SQL
    If <items> is a list, this function will assume it's a list of wm_id's
    If <items> is a single integer, this function will match that many items from the Walmart table
    Returns a list of dictionaries with the keys {wm_id, name, price, upc, in_stock, free_ship}
    """
    
    con = con_postgres()
    wmDicts = []
    
    go = False
    if items:
        if isinstance(items, int):
            go = True
            isList = False
        elif isinstance(items, list) or isinstance(items, tuple):
            if isinstance(items[0], int):
                go = True   
                isList = True      
    
    if go:    
        dict_cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)  # SELECT requests using this returns a list of dicts, with columns names as the keys
        
        sqlTxt = '''SELECT wm_id, name, price, upc, model, brand, in_stock, free_ship FROM "Prod_Wm" '''    
        
        # Sort out <items> depending on what type it is
        if isList:  # Write the wm_id's out into the WHERE clause of a SQL SELECT statement
            sqlSnippet = 'WHERE'
            for wm_id in items:
                sqlSnippet = sqlSnippet + " (wm_id = '" + str(wm_id) + "') OR"
            sqlSnippet = sqlSnippet[:-3]  # Remove the last 3 characters: ' or'
        else:  # Write the SQL bit to get the n most pertinent items
            sqlSnippet = 'ORDER BY fetched ASC FETCH FIRST %s ROWS ONLY' % items
        
        dict_cur.execute(sqlTxt + sqlSnippet)
        wmDicts = dict_cur.fetchall()  # List of dicts
    
    # Some error-logging
    if not wmDicts or not items:
        print("----------")
        if not wmDicts:  # No matches for the input wm_ids were found
            print("Error: walmartclasses.wm_db_query didn't find any matches for its wm_ids, which were: {}".format(items))
        if not items:  # No input wm_ids were given
            print("Error: walmartclasses.wm_db_query wasn't given any wm_ids in its parameters")
        print("----------")
    elif isinstance(items, list):
        if len(wmDicts) != len(items):  # Discrepency between the number of wm_ids input and output
            print("----------")
            wmDictsIds = [a["wm_id"] for a in wmDicts]
            wmDictsOnly = list(set(wmDictsIds) - set(items))  # wm_ids that are in in wmDicts, but not items
            itemsOnly = list(set(items) - set(wmDictsIds))  # wm_ids that are in items, but not wmDicts
            print("Error: walmartclasses.wm_db_query either returned extra wm_ids, or didn't find a match for each wm_id fed to it.")
            print("Extra wm_ids returned: {}".format(wmDictsOnly))
            print("wm_ids that were fed but didn't find a match: {}".format(itemsOnly))
            print("----------")
    
    for j in range(0, len(wmDicts)):  # Convert some text values to boolean, and add some entries
        if wmDicts[j]['in_stock'].lower() == 'available':
            wmDicts[j]['in_stock'] = 'true'
        else:
            wmDicts[j]['in_stock'] = 'false'
            
    if con:
        con.close()
    
    return wmDicts
    

def update_wm_data_timestamps():
    
    print('update_wm_data_timestamps - starting...')
    
    con = con_postgres()
    sqlTxt = '''SELECT b.asin, a.fetched
                FROM "Prod_Wm" as a
                INNER JOIN "Products_WmAz" as b
                ON a.wm_id = b.wm_id
                WHERE a.fetched IS NOT Null'''
    datums = call_sql(con, sqlTxt, [], 'executeReturn')
    
    if datums:
        record_timestamps(datums, 'wm_data')
    
    if con:
        con.close()
    
    print('update_wm_data_timestamps - finished')
        
        
def delete_bad_upcs():
    """
    Delete items with non-numeric UPCs from Prod_Wm
    """
    
    con = con_postgres()
    
    sqlTxt = '''DELETE FROM "Prod_Wm"
                WHERE ISNUMERIC(upc) is FALSE'''
    call_sql(con, sqlTxt, [], "executeNoReturn")
    
    if con:
        con.close()


def mark_wm_dups():
    """
    Mark Prod_Wm.dup as True for items that have non-unique UPCs
    """
    
    con = con_postgres()
    # Set all dups rows to False, then overwrite the ones that are upc duplicates with True
    sqlTxt = '''LOCK TABLE "Prod_Wm" IN SHARE ROW EXCLUSIVE MODE;
                UPDATE "Prod_Wm" AS a
                SET dup = False;
                UPDATE "Prod_Wm" AS a
                SET dup = True
                FROM (
                SELECT upc
                FROM "Prod_Wm"
                WHERE upc IS NOT Null
                GROUP BY upc
                HAVING count(*) > 1
                ) AS  subqry
                WHERE a.upc = subqry.upc'''
    call_sql(con, sqlTxt, [], "executeNoReturn")
    
    if con:
        con.close()
    

def update_wm_query_log(num, ts=None):
    """
    Upserts timestamps and num_queries in wm.WmQueryLog
    
    Parameters: num, ts
    """
    
    if not ts:
        ts = datetime_floor(5.0)
    
    # Update WmQueryLog. If queries have already been logged for the current timestamp, add to that total.
    sqlTxt = '''INSERT INTO wm."WmQueryLog" (timestamp, num_queries)
                VALUES(%s, %s)
                ON CONFLICT (timestamp) DO UPDATE
                SET num_queries = "WmQueryLog".num_queries + %s'''
    con = con_postgres()
    theData = [ts, num, num]
    call_sql(con, sqlTxt, theData, "executeNoReturn")
    

def xml_json_err_check(apiStr, xmlOrJson):
    """
    See if the Walmart API call returned an error code
    <apiStr> is the xml or json code as a string. <xmlOrJson> flags whether the <apiStr> value is xml or json
    Returns a dict which contains a flag for whether the <apiStr> tripped an error or not, as well as the data itself
    """
    
    # dt = str(datetime_floor(1.0/600)).replace(":", "-").replace("0000", "") #Super pro way of truncating some of the microsecond decimals off
    
    if apiStr is None:
        return {'isErr': True, 'datums': 'no_data_returned_from_api'}
    
    if xmlOrJson == 'xml':
        try:
            theXml = ET.fromstring(apiStr.encode('utf-8'))
        except Exception as d:
            print(d)
#             write_to_file("ET.fromstring error - {}.xml".format(dt), apiStr, dirrr='DataFiles', absPath=False)
            return {'isErr': True, 'datums': 'ET.fromstring_error'}
        
        else:
            try:
                errElem = theXml.findall("error/code")
            except ET.ParseError as d:
                print(d)
#                 write_to_file('unknown error - {}.xml'.format(dt), apiStr, dirrr='DataFiles', absPath=False)
                return {'isErr': True, 'datums': 'unknown_error'}
            
            else:  # Still don't know if there's an error or not
                try:
                    errTxt = errElem[0].text  # Not sure why this needs to be referenced as a list element, but it does
#                     write_to_file('{}.xml'.format(errTxt), apiStr, dirrr='DataFiles', absPath=False)
                    return {'isErr': True, 'datums': errTxt}
                except Exception:  # No errors
                    return {'isErr': False, 'datums': theXml}
    
    # If there's an error using json.loads, check the file as an xml, since Walmart always seems to return errors as xml
    elif xmlOrJson == 'json':
        try:
            theJson = json.loads(apiStr)
        except json.decoder.JSONDecodeError:
            return xml_json_err_check(apiStr, 'xml')
        
        else:
            try:
                errTxt = theJson['errors'][0]['code']
                return {'isErr': True, 'datums': errTxt}
            except Exception:  # No errors
                return {'isErr': False, 'datums': theJson}
