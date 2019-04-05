import csv
import io
import time
import xml.etree.ElementTree as ET
from decimal import Decimal
from operator import itemgetter

from boto.mws.connection import MWSConnection
from mwstools.mws_overrides import OverrideProducts

from AmazonSelling.tools import call_sql, write_to_file, record_timestamps, datetime_floor, chunks, make_sql_list, \
    get_credentials, con_postgres


class MWSManager:
    
    def __init__(self):

        self.apiKeys = get_credentials({'AmazonMWS': ('marketplaceID', 'merchantID', 'accessKeyID',
                                                      'secretKey')})['AmazonMWS']
        mws = MWSConnection(self.apiKeys['accessKeyID'], self.apiKeys['secretKey'], Merchant=self.apiKeys['merchantID'],
                            SellerId=self.apiKeys['merchantID'])
        
        self.botoFuncs = {
        'get_matching_product_for_id':        mws.get_matching_product_for_id,
        'get_competitive_pricing_for_asin':   mws.get_competitive_pricing_for_asin,
        'get_lowest_offer_listings_for_asin': mws.get_lowest_offer_listings_for_asin,
        'request_report':                     mws.request_report,
        'get_report_request_list':            mws.get_report_request_list,
        'get_report_list':                    mws.get_report_list,
        'get_report':                         mws.get_report,
        'list_inventory_supply':              mws.list_inventory_supply}
    
    def boto_call(self, operation, botoArgs=dict()):
        """
        Takes the MWS <operation> and its specific arguments as a dict in <botoArgs>
        Submits a query request to MWS and writes the response to an XML file
        Also returns the query result as an ElementTree root
        """

        # Needed to make boto response readable:
        # http://stackoverflow.com/questions/25503563/how-can-i-return-xml-from-boto-calls?rq=1
        MWSConnection._parse_response = lambda _s, _x, _y, z: z
        
        botoArgs['MarketplaceId'] = self.apiKeys['marketplaceID']
        
        try:
            response = self.botoFuncs[operation](**botoArgs)
        except Exception as err:
            print('----------\nThere was an error with operation: {}, args: {}\n{}\n----------'
                  .format(operation, botoArgs, err))
            return None        
        
        it = ET.iterparse(io.StringIO(response.decode('utf-8')))
        
        # Insert code here to parse result for errors
        '''root = it.root
        <parse>'''
        
        # This chunk's to get rid of the namespaces in the xml from Amazon. http://stackoverflow.com/a/33997423
        for _, el in it:
            if '}' in el.tag:
                el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
            for at in el.attrib.keys():  # strip namespaces of attributes too
                if '}' in at:
                    newat = at.split('}', 1)[1]
                    el.attrib[newat] = el.attrib[at]
                    del el.attrib[at]
          
        root = it.root
        
        # Write data to file in <relative path>/DataFiles/<operation>.xml
        write_to_file('%s.xml' % operation, (ET.tostring(root, encoding="us-ascii", method="xml")).decode('utf-8'),
                      dirrr='DataFiles', absPath=False)
        
        return root


class Products:

    def match_to_az(self, source, idType, ids):
        """
        Matches a source's products to Amazon products and writes/updates columns in Products_WmAz.
        When multiple matches are found for a UPC, writes relevant data to SQL (Matcher_WmAz) to be looked at by a
        different subroutine.
        <source> is a string containing the name of the source.
        <id_type> defines which IdType will be used for GetMatchingProductForId ('upc', 'asin', etc.)
        <ids> is a list/tuple of wm_ids, if <idType> is 'upc'
        <ids> is a list/tuple of dicts with keys 'wm_id' and also 'asin'/whatever for idTypes other than 'upc'
        """
        
        from Walmart.walmartclasses import wm_db_query
        
        if idType == 'upc':
            wm_ids = ids
        else:
            wm_ids = tuple(b['wm_id'] for b in ids)
        
        sourceDicts = wm_db_query(wm_ids)
        upcs = tuple(i['upc'] for i in sourceDicts)
        
        # Create <theParams> using <idType> & <ids>
        if idType == 'upc':
            theParams = {'IdType': 'UPC', 'IdList': upcs}
        else:  # Use <idType> to create <mwsIdType> & <theParams>
            mwsIdType = idType.upper()
            theParams = {'IdType': mwsIdType, 'IdList': [g[idType] for g in ids]}
        
        '''Match the Walmart product data to Amazon using the IdList (probably UPCs or ASINs)'''
        root = MWSManager.boto_call(MWSManager(), 'get_matching_product_for_id', theParams)
    
        '''Parse the xml and write the data to SQL'''
        con = con_postgres()
        
        marketplaceAsinTags = ["ASIN"]
        itemAttributesTags = ["Title", "Brand"]  # , "Model", "NumberOfItems", "PackageQuantity"]
#         relationshipsTags = ["ASIN"]
        
        azKeys = ["ASIN", "Title", "Brand", "var_parent", "Rank_1", "ProductCategoryId_1", "Rank_2",
                  "ProductCategoryId_2", "Rank_3", "ProductCategoryId_3", "Rank_4", "ProductCategoryId_4"]
        if source == 'Walmart':
            sourceKeys = ["wm_id", "name", "price", "upc", "model", "brand", "in_stock", "free_ship"] 
        
        theData = []
        if root is not None:
            
            # Parse through the xml and get everything I want
            for result in root.iter('GetMatchingProductForIdResult'):
                # Initializes a dictionary with keys from sourceKeys, and all values as None
                sourceValues = dict.fromkeys(sourceKeys)
                xmlCongl = []
                
                try:
                    resultId = str(result.attrib['Id'])
                except ValueError:  # Couldn't cast to integer
                    continue
                
                if idType == 'upc':  # Matches a product from the Amazon xml to the Wm item, using the UPC or other ID
                    for sourceItem in sourceDicts:
                        if resultId == sourceItem['upc']:
                            sourceValues = sourceItem
                            break
                elif idType == 'asin':
                    for idDict in ids:  # Match <resultID> to <ids>
                        if resultId == idDict[idType]:
                            for sourceItem in sourceDicts:  # Match <ids> to <sourceDicts>
                                if str(idDict['wm_id']) == str(sourceItem['wm_id']):
                                    sourceValues = sourceItem
                                    break
                            break
                 
                for product in result.iter('Product'):  # Using iter, since each ID can match multiple ASINs
                    # Initializes a dictionary with keys from azKeys, and all values as None
                    azValues = {key: None for key in azKeys}
                     
                    u = product.find('.//Identifiers/MarketplaceASIN')      
                    for y in list(u):
                        if y.tag in marketplaceAsinTags:
                            azValues[y.tag] = y.text
                             
                    v = product.find('.//AttributeSets/ItemAttributes')
                    for y in list(v):
                        if y.tag in itemAttributesTags:
                            azValues[y.tag] = y.text
                     
    #               #This is for if I need to get multiple variations, rather than just one
    #                     azValues['Alt_ASINs'] = None
    #                     # There can be multiple VariationParents?? VariationChildren, yes.
    #                     for variation in product.iter('VariationParent'):
    #                         asinsTemp = list()
    #                         for y in variation.findall('.//Identifiers/MarketplaceASIN'):
    #                             for z in list(y):
    #                                 if z.tag in relationshipsTags:
    #                                     asinsTemp.append(z.text)
    #                                     # Comma-separated string of variation ASINs
    #                                     azValues['var_parent'] = ','.join(asinsTemp)
                    
                    w = product.find('.//Relationships/VariationParent/Identifiers/MarketplaceASIN')
                    if w is not None:
                        for y in list(w):
                            if y.tag == "ASIN":
                                azValues['var_parent'] = y.text
                     
                    numSalesRanks = 0
                    for salesRank in product.iter('SalesRank'):
                        numSalesRanks += 1
                        if numSalesRanks > 4:
                            # print ('{} has more than 4 sales ranks!'.format(azValues["ASIN"]))
                            break
                        for y in list(salesRank):
                            if y.tag == 'Rank':
                                libKey = 'Rank_%s' % numSalesRanks
                                print(libKey)
                                azValues[libKey] = y.text
                            if y.tag == 'ProductCategoryId':
                                libKey = 'ProductCategoryId_%s' % numSalesRanks
                                print(libKey)
                                azValues[libKey] = y.text
                     
#                     azValues["url"] = 'amazon.com/dp/{}'.format(str(azValues["ASIN"]))
#                     azValues["ccc"] = 'camelcamelcamel.com/product/{}?active=sales_rank'.format(str(azValues["ASIN"]))
                    
                    # This is for writing to Matcher_WmAz, in case that needs to happen for this ID
                    if idType != 'asin':
                        xmlCongl.append([str(sourceValues["wm_id"]) + azValues['ASIN'],
                                         sourceValues["upc"],
                                         sourceValues["wm_id"],
                                         sourceValues["name"],
                                         sourceValues["price"],
                                         sourceValues["model"],
                                         sourceValues["brand"],
                                         azValues['ASIN'],
                                         ET.tostring(v, encoding='unicode'),
                                         ET.tostring(product.find('.//Relationships'), encoding='unicode'),
                                         ET.tostring(product.find('.//SalesRankings'), encoding='unicode'),
                                         # ON CONFLICT DO UPDATE values start here
                                         sourceValues["upc"],
                                         sourceValues["name"],
                                         sourceValues["price"],
                                         sourceValues["model"],
                                         sourceValues["brand"],
                                         ET.tostring(v, encoding='unicode'),
                                         ET.tostring(product.find('.//Relationships'), encoding='unicode'),
                                         ET.tostring(product.find('.//SalesRankings'), encoding='unicode')])
                    
                    # This is for writing to Products_WmAz
                    theData.append((azValues["ASIN"],
                                    azValues["Title"],
                                    sourceValues["wm_id"],
                                    sourceValues["name"],
                                    sourceValues["price"],
                                    sourceValues["upc"],
                                    azValues["Brand"],
                                    sourceValues["in_stock"],
                                    sourceValues["free_ship"],
                                    azValues["Rank_1"],
                                    azValues["ProductCategoryId_1"],
                                    azValues["Rank_2"],
                                    azValues["ProductCategoryId_2"],
                                    azValues["Rank_3"],
                                    azValues["ProductCategoryId_3"],
                                    azValues["Rank_4"],
                                    azValues["ProductCategoryId_4"],
                                    azValues["var_parent"],
                                    # ON CONFLICT DO UPDATE values start here
                                    sourceValues["price"],
                                    sourceValues["in_stock"],
                                    sourceValues["free_ship"],
                                    azValues["var_parent"],
                                    azValues["Rank_1"],
                                    azValues["ProductCategoryId_1"],
                                    azValues["Rank_2"],
                                    azValues["ProductCategoryId_2"],
                                    azValues["Rank_3"],
                                    azValues["ProductCategoryId_3"],
                                    azValues["Rank_4"],
                                    azValues["ProductCategoryId_4"]))
                    
                # If the UPC yielded multiple Az matches, write relevant data to Matcher_WmAz to be looked at
                # later by a different subroutine
                if len(xmlCongl) > 1 and idType != 'asin':
                    theData = theData[:-len(xmlCongl)]  # Remove last n elements from theData, where n = len(xmlCongl)
                    # This is so the results of UPCs with multiple matches are ommited from Products_WmAz, until
                    # matcher can find the one true match.

                    sqlTxt = '''INSERT INTO "Matcher_WmAz" (unique_id, upc, wm_id, wm_name, wm_price, wm_model,
                                wm_brand, asin, item_attribs, relationships, sales_ranks)
                                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT ("unique_id") DO UPDATE
                                SET upc = %s, wm_name = %s, wm_price = %s, wm_model = %s, wm_brand = %s,
                                item_attribs = %s, relationships = %s, sales_ranks = %s'''
                    call_sql(con, sqlTxt, xmlCongl, "executeBatch")
            
            # Update Products_WmAz
            sqlTxt = '''INSERT INTO "Products_WmAz" (asin, az_name, wm_id, wm_name, wm_price, upc, az_brand, wm_instock,
                        free_ship, salesrank1, catid1, salesrank2, catid2, salesrank3, catid3, salesrank4, catid4,
                        var_parent)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT ("asin") DO UPDATE
                        SET wm_price = %s, wm_instock = %s, free_ship = %s, var_parent = %s, salesrank1 = %s,
                        catid1 = %s, salesrank2 = %s, catid2 = %s, salesrank3 = %s, catid3 = %s, salesrank4 = %s,
                        catid4 = %s'''
            if theData:
                theData.sort(key=itemgetter(0))
            call_sql(con, sqlTxt, theData, 'executeBatch')
            
        # Update Prod_Wm.last_matched
        sqlTxt = '''UPDATE "Prod_Wm"
                    SET last_matched = %s
                    WHERE wm_id = %s'''
        if wm_ids:
            theData2 = sorted([(datetime_floor(1.0/60), wmId) for wmId in wm_ids], key=itemgetter(1))
        call_sql(con, sqlTxt, theData2, 'executeBatch')
        if con:
            con.close()
        
        if not theData:
            if idType == 'upc':
                print('No Amazon matches for UPCs {0}, or all matches were with multiple ASINs, so they were omitted'
                      .format(upcs))
            else:
                print('No Amazon matches for {0}s {1}, or all matches were with multiple ASINs, so they were omitted'
                      .format(mwsIdType, theParams['IdList']))
        else:
            record_timestamps([hnng[0] for hnng in theData], 'match_to_az')
    
    def get_comp_pricing(self, asins):
        """
        Using ASINs, retrieves GetCompetitivePricingForASIN from Amazon and writes it to SQL
        <asins> is a list of ASINs
        Retrieves both pricing and sales ranks
        """
        
        theParams = {'ASINList': asins}
        root = MWSManager.boto_call(MWSManager(), 'get_competitive_pricing_for_asin', theParams)
        
        '''Write the Amazon replies to SQL'''
        
        azKeys = ["CompetPrice", "Rank_1", "ProductCategoryId_1", "Rank_2", "ProductCategoryId_2",
                  "Rank_3", "ProductCategoryId_3", "Rank_4", "ProductCategoryId_4", "TradeIn", "ASIN"]
        
        theData = []
        # Parse through the xml and get everything I want
        for result in root.iter('GetCompetitivePricingForASINResult'):
            azValues = dict.fromkeys(azKeys)  # Initializes a dictionary with keys from azKeys, and all values as None
            azValues['ASIN'] = result.attrib['ASIN']
            
            foundNewPrice = False
            for w in result.findall('.//Product/CompetitivePricing/CompetitivePrices/CompetitivePrice'):
                if w.attrib['condition'] == 'New':
                    try:
                        azValues['CompetPrice'] = w.find('.//Price/LandedPrice/Amount').text
                    except AttributeError:  # Sometimes LandedPrice is missing
                        azValues['CompetPrice'] = w.find('.//Price/ListingPrice/Amount').text
                    foundNewPrice = True
            if not foundNewPrice:
                azValues['CompetPrice'] = None
                
            foundTradeIn = False
            for w in result.findall('.//Product/CompetitivePricing/TradeInValue'):
                azValues['TradeIn'] = w.find('.//Amount').text
                foundTradeIn = True
            if not foundTradeIn:
                azValues['TradeIn'] = None
                
            numSalesRanks = 0
            for salesRank in result.iter('SalesRank'):
                numSalesRanks += 1
                if numSalesRanks > 4:
                    # print('{} has more than 4 sales ranks!'.format(azValues["ASIN"]))
                    break
                for y in list(salesRank):
                    if y.tag == 'Rank':
                        libKey = 'Rank_%s' % numSalesRanks
                        azValues[libKey] = y.text
                    if y.tag == 'ProductCategoryId':
                        libKey = 'ProductCategoryId_%s' % numSalesRanks
                        azValues[libKey] = y.text
            
            theData.append(tuple(azValues.values()))
        
        sqlTxt = '''UPDATE "Products_WmAz"
                    SET comp_price = %s, salesrank1 = %s, catid1 = %s, salesrank2 = %s, catid2 = %s, salesrank3 = %s,
                    catid3 = %s, salesrank4 = %s, catid4 = %s, trade_in = %s
                    WHERE asin = %s'''
        if theData:
            theData.sort(key=itemgetter(-1))
        con = con_postgres()
        call_sql(con, sqlTxt, theData, 'executeBatch')
        if con:
            con.close()
            
        if asins:
            record_timestamps(asins, 'az_comp_price')
        
    def get_lowest_offer_listings(self, asins):
        """
        Using ASINs, retrieves GetLowestOfferListingsForASIN from Amazon and writes it to SQL
        <asins> is a list of ASINs
        Retrieves the lowest FBA offer and the lowest merchant fulfilled offer, new only
        """
        
        theParams = {'ASINList': asins, 'ItemCondition': 'New'}
        root = MWSManager.boto_call(MWSManager(), 'get_lowest_offer_listings_for_asin', theParams)
        
        '''Write the Amazon replies to SQL'''
        
        azKeys = ["Amazon", "Merchant", "ASIN"]
        
        theData = []
        # Parse through the xml and get everything I want
        for result in root.iter('GetLowestOfferListingsForASINResult'):
            azValues = dict.fromkeys(azKeys)  # Initializes a dictionary with keys from azKeys, and all values as None
            azValues['ASIN'] = result.attrib['ASIN']
            
            channel = None
            for w in result.findall('.//Product/LowestOfferListings/LowestOfferListing'):
                channel = w.find('.//Qualifiers/FulfillmentChannel').text
                price = w.find('.//Price/LandedPrice/Amount').text
                
                if azValues[channel] is None:
                    azValues[channel] = price
                else:
                    azValues[channel] = min(float(price), float(azValues[channel]))
            
            theData.append(tuple(azValues.values()))
        
        sqlTxt = '''UPDATE "Products_WmAz"
                    SET lowest_fba = %s, lowest_merch = %s
                    WHERE asin = %s'''
        if theData:
            theData.sort(key=itemgetter(-1))
        con = con_postgres()
        call_sql(con, sqlTxt, theData, "executeBatch")
        if con:
            con.close()
            
        if asins:
            record_timestamps(asins, "az_lowest_offer")
            
    def get_fees_est(self, asins):
        """
        Using ASINs, retrieves pricing from Amazon and writes it to SQL
        <asins> is a list or tuple of asins.
        Currently, can only take 4 ASINs at a time
        Also updates the my_price column in Products_WmAz for the passed asins
        """
        
        from mwstools.parsers.products.get_my_fees_estimate import GetMyFeesEstimateResponse
        
        con = con_postgres()
        
        # Get my theoretical price for each item
        myPrices = get_my_price(asins)
        inputs = tuple((q, myPrices[q],) for q in myPrices if myPrices[q])  # Remove entries that are lacking a price
        if not inputs:  # Oops, now there's no entries still remaining
            record_timestamps(asins, 'az_fees')
            return
        
        api = OverrideProducts(MWSManager().apiKeys['accessKeyID'], MWSManager().apiKeys['secretKey'],
                               MWSManager().apiKeys['merchantID'])
        estimate_requests = [api.gen_fees_estimate_request(MWSManager().apiKeys['marketplaceID'], x[0], identifier=x[0],
                                                           listing_price=x[1]) for x in inputs]
        try:
            response = api.get_my_fees_estimate(estimate_requests)
        except Exception as err:
            print('Error with get_my_fees_estimate in the mwstools library:\ninputs:{}\n{}'.format(inputs, err))
            return
        
        # Write data to file in <relative path>/DataFiles/<operation>.xml
        write_to_file('get_my_fees_estimate.xml', response.text, dirrr='DataFiles', absPath=False)
        
        theData = []
        for w in GetMyFeesEstimateResponse.load(response.text).fees_estimate_result_list:
            if w.error.code:
                theData.append((w.listing_price, -1.0, w.id_value))
            else:
                theData.append((w.listing_price, w.total_fees_estimate, w.id_value))
        
        sqlTxt = '''UPDATE "Products_WmAz"
                    SET my_price = %s, fees_est = %s
                    WHERE asin = %s'''
        if theData:
            theData.sort(key=itemgetter(-1))
        call_sql(con, sqlTxt, theData, 'executeBatch')
        
        if con:
            con.close()
        
        if inputs:
            record_timestamps(asins, 'az_fees')
            
    
class Reports:
    
    def grab_report(self, reportParams):
        
        # Request the report and get its <ReportRequestId>
        reqId = self.request_report(reportParams)
        
        # Continuously run get_report_request_list() until the report with our reqId is ready
        if reqId:
            reportReady = False
            while not reportReady:
                prev = time.time()
                print('Running get_report_request_list()...')
                reqsDict = self.get_report_request_list()
                
                # Once get_report_request_list()'s return dict includes <reqId>, save that dict and exit the while loop
                for reqIdFromList in reqsDict:
                    if reqIdFromList == reqId:
                        if reqsDict[reqIdFromList]['ReportProcessingStatus'] == '_DONE_':
                            reportReady = True
                        else:
                            errTxt = "get_report_request_list() returned an unworkable <ReportProcessingStatus> value" \
                                     "for <ReportRequestId> '{0}'. ".format(reqIdFromList)
                            errTxt += "The returned value was '{0}'"\
                                      .format(reqsDict[reqIdFromList]['ReportProcessingStatus'])
                            print(errTxt)
                            raise RuntimeError(errTxt, reqsDict[reqIdFromList]['ReportProcessingStatus'])
                        break
                
                if reportReady:
                    break
                time.sleep(max(prev + 46 - time.time(), 0))
        else:
            print('request_report() failed to create a ReportRequestId. Not sure what happened')
            return
        
        # If get_report_request_list()'s return dict didn't include a <GeneratedReportId> for <reqId>,
        # run get_report_list() to get the <ReportId>
        if 'GeneratedReportId' not in reqsDict[reqIdFromList]:
            repsDict = self.get_report_list()
            reqsRepsDict = {}
            
            # Gather the values for each report_request_id from <reqsDict> and <repsDict>
            for theId in set(tuple(reqsDict.keys()) + tuple(repsDict.keys())):  # A combined set of <ReportRequestId>s
                reqRepDict = None
                if theId in reqsDict and theId in repsDict:
                    # Merge <reqsDict>'s and <repsDict>'s entries for this report_request_id
                    # stackoverflow.com/questions/38987/how-to-merge-two-dictionaries-in-a-single-expression
                    reqRepDict = {**reqsDict[theId], **repsDict[theId]}
                elif theId in reqsDict:
                    reqRepDict = reqsDict[theId]
                elif theId in repsDict:
                    reqRepDict = repsDict[theId]
                    
                if reqRepDict:
                    reqsRepsDict[theId] = reqRepDict
            
            thisReqRep = reqsRepsDict[reqId]
        
        else:
            thisReqRep = reqsDict[reqId]
        
        if 'GeneratedReportId' in thisReqRep:
            repId = thisReqRep['GeneratedReportId']
        else:
            repId = thisReqRep['ReportId']
        
        theReport = self.get_report(repId)
        
        a = 1
        
        print(a, theReport)
            
    def request_report(self, reportParams):
        
        # return '50122017449'
        
        root = MWSManager.boto_call(MWSManager(), 'request_report', reportParams)
        
        try:
            reportRequestId = root.find('.//ReportRequestId').text
        except:
            return None
        
        if reportRequestId:
            return reportRequestId
        else:
            return None
    
    def get_report_request_list(self):

        # theParams = None
        theParams = {}
        root = MWSManager.boto_call(MWSManager(), 'get_report_request_list', theParams)
        
        # Not all reports will generate a <GeneratedReportId>
        reqsDict = {}  # <ReportRequestId>: {all fieldnames: values for each report request}
        
        # Go through the xml and pull out the relevant data from each <ReportRequestInfo>
        for result in root.iter('ReportRequestInfo'):
            
            reqId = result.find('.//ReportRequestId').text
            reqDict = {v.tag: v.text for v in list(result)}
            reqsDict[reqId] = reqDict
            
        return reqsDict
    
    def get_report_list(self):

        # theParams = None
        theParams = {}
        root = MWSManager.boto_call(MWSManager(), 'get_report_list', theParams)
        repsDict = {}
        
        # Go through the xml and pull out the relevant data from each <ReportRequestInfo>
        for result in root.iter('ReportInfo'):
            
            reqId = result.find('.//ReportRequestId').text
            repDict = {v.tag: v.text for v in list(result)}
            repsDict[reqId] = repDict
            
        return repsDict
    
    def get_report(self, repId):

        """
        api = OverrideReports(MWSManager().accessKeyID, MWSManager().secretKey, MWSManager().merchantID)

        try:
            response = api.get_report(report_id=repId)
        except Exception as err:
            print ("Error with get_report in the mwstools library:\nreport_id: {}\n{}".format(repId, err))
            return

        #Write data to file in <relative path>/DataFiles/get_report.txt
        write_to_file('get_report.txt', response.text, dirrr = 'DataFiles', abspath = False)

        theDatums = io.StringIO(response.text)
        """
        
        theDatums = open('H:\\Arbitrage\\Amazon\\DataFiles\\get_report.txt', 'r', newline='\n')
        
        # c = csv.reader(theDatums, delimiter='\t')
        c = csv.DictReader(theDatums, delimiter='\t')
        for row in c:
            # if row:
            print(row)

        return 'fake return'


class FulfillmentInventory:
    
    def get_list_inventory_supply(self, skus):
        """
        Gets my current FBA inventory data from MWS and writes it to SQL in io.SKUs
        Requires: skus (tuple, list, or set)
        """
        
        theParams = {'SellerSkus': skus}
        root = MWSManager.boto_call(MWSManager(), 'list_inventory_supply', theParams)
        
        tagsTupl = ('SellerSKU', 'FNSKU', 'InStockSupplyQuantity', 'TotalSupplyQuantity')
        sqlOrder = ('fnsku', 'processing', 'available', 'sku')
        theData = []
        for result in root.iter('member'):
            w = {y.tag: y.text for y in list(result) if y.tag in tagsTupl}
            
            x = {}
            if 'SellerSKU' in w:
                x['sku'] = w['SellerSKU']
                
                if 'FNSKU' in w:
                    x['fnsku'] = w['FNSKU']
                else:
                    x['fnsku'] = None
                
                if 'InStockSupplyQuantity' in w:
                    x['available'] = int(w['InStockSupplyQuantity'])
                else:
                    x['available'] = 0
                    
                if 'TotalSupplyQuantity' in w:
                    x['processing'] = int(w['TotalSupplyQuantity']) - x['available']
                else:
                    x['processing'] = 0
            else:
                continue
            
            if x:
                theData.append(tuple(x[i] for i in sqlOrder))
            
        if theData:
            sqlTxt = '''UPDATE "SKUs"
                        SET fnsku = %s, processing = %s, available = %s
                        WHERE sku = %s'''
            con = con_postgres()
            call_sql(con, sqlTxt, theData, "executeBatch")
            if con:
                con.close()

        
class FulfillmentInboundShipment:
    
    def get_inbound_guidance_for_sku(self, skus):
        pass
    
#         #NOT SUPPORTED BY BOTO
#         #This will have to use python-amazon-mws
#         #See https://github.com/python-amazon-mws/python-amazon-mws/pull/22
#         '''
#         Advice from Amazon on whether a given sku should be sent in to FBA. Writes to io.SKUs.
#         Requires: skus (tuple, list, or set)
#         '''
#         
#         theParams = {'SellerSKUList': skus}
#         root = MWSManager.boto_call(MWSManager(), 'inbound_guidance_for_sku', theParams)
#         
#         #The rest has just been copied from get_list_inventory_supply
#         
#         tagsTupl = ('SellerSKU', 'FNSKU', 'InStockSupplyQuantity', 'TotalSupplyQuantity')
#         sqlOrder = ('fnsku', 'processing', 'available', 'sku')
#         theData = []
#         for result in root.iter('member'):
#             w = {y.tag: y.text for y in list(result) if y.tag in tagsTupl}
#             
#             x = {}
#             if 'SellerSKU' in w:
#                 x['sku'] = w['SellerSKU']
#                 
#                 if 'FNSKU' in w:
#                     x['fnsku'] = w['FNSKU']
#                 else:
#                     x['fnsku'] = None
#                 
#                 if 'InStockSupplyQuantity' in w:
#                     x['available'] = int(w['InStockSupplyQuantity'])
#                 else:
#                     x['available'] = 0
#                     
#                 if 'TotalSupplyQuantity' in w:
#                     x['processing'] = int(w['TotalSupplyQuantity']) - x['available']
#                 else:
#                     x['processing'] = 0
#             else:
#                 continue
#             
#             if x: theData.append(tuple(x[i] for i in sqlOrder))
#             
#         if theData:
#             sqlTxt = '''UPDATE "SKUs"
#                         SET fnsku = %s, processing = %s, available = %s
#                         WHERE sku = %s'''
#             con = con_postgres()
#             call_sql(con, sqlTxt, theData, "executeBatch")
#             if con:
#                 con.close()

    
def matcher(upc):
    pass

    import re  # , regex
    
    con = con_postgres()
    
    '''Retrieve data from Matcher_WmAz'''
    
    sqlTxt = '''SELECT *
                FROM "Matcher_WmAz"
                WHERE upc = %s'''
    a = call_sql(con, sqlTxt, [upc], "executeReturn", dictCur=True)
    
    azKeys = ['asin', 'item_attribs', 'relationships', 'sales_ranks']
    # Make a dict of all the values that are common between each item (basically all the Walmart data)
    # Make a dict of all the values that are unique to each item, i.e. the Amazon data
    # The keys for both dicts are the same as the column names from Matcher_WmAz
    cmnVals = {h: a[0][h] for h in a[0] if h not in azKeys and h != 'unique_id'}
    uniqVals = [{j: item[j] for j in item if j in azKeys} for item in a]
    
    wantedAttribRootTags = ['Title', 'Model', 'PartNumber', 'ItemPartNumber', 'Brand', 'Manufacturer', 'Label',
                            'Publisher', 'PackageQuantity', 'NumberOfItems']
    
    '''Parse the data out of the xml'''
    
    for azItem in uniqVals:
        attribRoot = ET.fromstring(azItem['item_attribs'])
        relatRoot = ET.fromstring(azItem['relationships'])
        ranksRoot = ET.fromstring(azItem['sales_ranks'])
        
        # Attributes
        for u in list(attribRoot):
            if u.tag in wantedAttribRootTags:
                azItem['az_' + u.tag] = u.text
        
        # VariationParent
        w = relatRoot.find('.//VariationParent/Identifiers/MarketplaceASIN')
        if w is not None:
            for bbb in list(w):
                if bbb.tag == "ASIN":
                    azItem['az_var_parent'] = bbb.text
        
        # VariationChilds
        # numChildren = 0
        numChildsTemp = []
        for variation in relatRoot.iter('VariationChild'):
            # numChildren += 1
            y = variation.find('.//Identifiers/MarketplaceASIN')
            for z in list(y):
                if z.tag == 'ASIN':
                    numChildsTemp.append(z.text)
#                     libKey = 'var_child_{}'.format(numChildren)
#                     azItem['az_' + libKey] = z.text
        azItem['az_var_childs'] = ','.join(numChildsTemp)
        
        # SalesRanks
        numSalesRanks = 0
        for salesRank in ranksRoot.iter('SalesRank'):
            numSalesRanks += 1
            if numSalesRanks <= 4:
                for v in list(salesRank):
                    if v.tag == 'Rank':
                        libKey = 'Rank_{}'.format(numSalesRanks)
                        azItem['az_' + libKey] = v.text
                    if v.tag == 'ProductCategoryId':
                        libKey = 'ProductCategoryId_{}'.format(numSalesRanks)
                        azItem['az_' + libKey] = v.text
    
    '''Use regex to extract multi-quantity numbers out of both the Walmart and Amazon product titles'''
    
    # For info on the (?s:.*?), see https://stackoverflow.com/a/33233868/5253431
    multPatterns = [r'set.of.(\d+)',
                    r'(?s:.*?)(\d+).*?set',
                    r'pack.of.(\d+)',
                    r'box.of.(\d+)',
                    r'bag.of.(\d+)',
                    r'(\d+).pc',
                    r'(\d+).piece',
                    r'(\d+).drops',
                    r'(?s:.*?)(\d+).*?count',
                    r'(?s:.*?)(\d+).*?ct',  # <8 ct>, <8ct>
                    r'(?s:.*?)(\d+).*?cnt',
                    r'(?s:.*?)(\d+).*?pack',  # Batteries, 9V, 4 Batteries/Pack
                    r'(?s:.*?)(\d+).*?pk',
                    r'(?s:.*?)(\d+).*?pck',
                    r',\s(\d+)',  # Rosemary concentrate, 4
                    r'(?s:.*?)(\d+).*?box',  # Adhesive, 2-1/4 x 4, 250/Box
                    r'(?s:.*?)(\d+).*?bx',
                    r'(\d+).bag',
                    r'(\d+).ea',
                    r'(\d+).sheet',
                    r'(\d+).pair',
                    r'(\d+).capsule',
                    r'\s\((\d+)\)'  # Wet Dog Food, (8)
                    ]
    
    # Try to extract a multi-quantity number from the Walmart product name
    if 'wm_name' in cmnVals:
        b = []  # A list of all the regex matches for the Walmart product. A list of dicts with keys 'value', 'index'
        for pattern in multPatterns:
            m = None
            
            # Using finditer, <m> will contain the last regex match for this pattern
            for m in re.finditer(pattern, cmnVals['wm_name'], re.I):
                pass
            if m:
                # Make sure it's an integer. This is kinda futile since finditer only returns a single digit...
                if Decimal(m.group(1)) % 1 == 0:
                    print('"{}" found "{}" in "{}", <index: {}> <value: "{}">'
                          .format(pattern, m.group(0), cmnVals['wm_name'], m.start(), m.group(1)))
                    b.append({'value': int(m.group(1)), 'index': m.start()})
        if not b:
            print('No multi-number found for Walmart item "{}"'.format(cmnVals['wm_name']))
        else:
            # Save only the regex match for this Walmart product that had the highest index,
            # i.e. started nearest to the end of the product title
            cmnVals['wm_MultNum'] = max(b, key=lambda d: d['index'])['value']
    else:
        cmnVals['wm_name_isMissing'] = True
    
    # Try to extract a multi-quantity number from each Amazon product name
    for prod in uniqVals:
        if 'az_Title' in prod:
            b = []  # A list of all the regex matches for this <prod>. A list of dicts with keys 'value', 'index'
            for pattern in multPatterns:
                m = None

                # Using finditer, <m> will contain the last regex match for this pattern
                for m in re.finditer(pattern, prod['az_Title'], re.I):
                    pass
                if m:
                    # Make sure it's an integer. This is kinda futile since finditeronly returns a single digit...
                    if Decimal(m.group(1)) % 1 == 0:
                        print('"{}" found "{}" in "{}", <index: {}> <value: "{}">'
                              .format(pattern, m.group(0), prod['az_Title'], m.start(), m.group(1)))
                        b.append({'value': int(m.group(1)), 'index': m.start()})                   
            if not b:
                print('No multi-number found for Amazon item "{}"'.format(prod['az_Title']))
            else:
                # Save only the regex match for this <prod> that had the highest index, i.e. started nearest to the end of the product title
                prod['az_MultNum'] = max(b, key=lambda d: d['index'])['value']
        else:
            prod['az_Title_isMissing'] = True


def update_mws_log(self, timestamp, callType, count):
    """
    No longer in use. Was used to update the MWSLog table in postgres, similar to WmQueryLog, but since
    MWS calls are throttled and restored so quickly, no database is needed. The throttling info can
    just be stored in memory.
    """
    
    if callType not in self.throttleLimits:
        print('The function {} did not receive a suitable argument for <callType>. What it got was: {}: {}'
              .format(update_mws_log.__name__, type(callType), callType))
        return
    
    ts = datetime_floor(1.0, theTs=timestamp)  # One-minute intervals
    callType = callType.lower()
    
    con = con_postgres()
    
    # Update MWSLog. If queries have already been logged for the current timestamp & callType, add to that total.
    sqlTxt = '''INSERT INTO "MWSLog" ("timestamp", "{}")
                VALUES(%s, %s)
                ON CONFLICT ("timestamp") DO UPDATE
                SET "{}" = COALESCE("MWSLog"."{}", 0) + %s'''.format(callType, callType, callType)
    theData = [ts, count, count]
    call_sql(con, sqlTxt, theData, "executeNoReturn")
    
    if con:
        con.close()
        
    
def get_my_price(theAsins):
    """
    Uses comp_price, lowest_fba, and lowest_merch to determine what my price would be for an asin.
    <theAsins> can be a single asin or a list/tuple of asins.
    Returns a dictionary of asins:prices
    """
    
    # Convert theAsins into a tuple
    if not (isinstance(theAsins, list) or isinstance(theAsins, tuple)):
        asins = (theAsins,)
    else:
        asins = tuple(theAsins)
    
    # Retrieve pricing data from SQL
    con = con_postgres()
    sqlTxt = '''SELECT asin, comp_price, lowest_fba, lowest_merch
                FROM "Products_WmAz"
                WHERE asin IN ({})'''.format(",".join(["'" + str(i) + "'" for i in asins]))
    allPrices = call_sql(con, sqlTxt, [], "executeReturn", dictCur=True)
    
#     allPrices = [{"asin": "QADASDW", "comp_price": None, "lowest_fba": None, "lowest_merch": 10},
#                  {"asin": "ASD978", "comp_price": None, "lowest_fba": 5.57, "lowest_merch": None}]
    
    # Go through and determine what my price would be for each item
    myPrices = {}
    buyBoxMult = Decimal("1.15")  # Multiplier for my price to still win bb over the lowest merchant fulfilled offer
    noBuyBoxMult = Decimal("1.15")  # Multiplier for my price over the lowest MF offer when there is no bb
    for asin in allPrices:        
        if asin["comp_price"]:
            if asin["lowest_fba"] and asin["lowest_merch"]:
                myPrice = min(asin["comp_price"], asin["lowest_fba"], buyBoxMult * asin["lowest_merch"])
            elif asin["lowest_fba"]:
                myPrice = asin["lowest_fba"]
            elif asin["lowest_merch"]:
                # Sometimes MWS will return a comp_price that corresponds to Amazon's own listing (shipped and sold by),
                # but won't return the corresponding lowest_fba price. This makes going through manualSh a pain. To deal
                # with this, just consider comp_price in addition to lowest_merch
                myPrice = min(asin["comp_price"], buyBoxMult * asin["lowest_merch"])
            else:  # This shouldn't ever happen, but just in case
                myPrice = asin["comp_price"]
        else:  # No buy box
            if asin["lowest_fba"] and asin["lowest_merch"]:
                myPrice = min(asin["lowest_fba"], noBuyBoxMult * asin["lowest_merch"])
            elif asin["lowest_fba"]:
                myPrice = asin["lowest_fba"]
            elif asin["lowest_merch"]:
                myPrice = noBuyBoxMult * asin["lowest_merch"]
            else:
                myPrice = None
        
        if myPrice is not None:
            myPrices[asin['asin']] = round(myPrice, 2)
        else:
            myPrices[asin['asin']] = None
        
    return myPrices
        

def calc_sales_rank(asins, theTable='Products_WmAz'):
    """
    #Calculate the salesrank % for each product, write to SQL
    amazon.com/s/ref=sr_hi_1?rh=n%3A1055398%2Ck%3A-fghfhf&keywords=-fghfhf&ie=UTF8&qid=1497942532
    amazon.com/s/ref=nb_sb_noss?url=search-alias%3Dkitchen&field-keywords=-fghfhf
    amazon.com/s/ref=nb_sb_noss?url=search-alias%3Dtoys-and-games&field-keywords=-fghfhf&rh=n%3A165793011%2Ck%3A-fghfhf
    """
    
    deptDict = {"art_and_craft_supply_display_on_website": "Arts, Crafts & Sewing",                
                "automotive_display_on_website":           "Automotive Parts & Accessories",                               
                "baby_product_display_on_website":         "Baby",
                "beauty_display_on_website":               "Beauty & Personal Care", 
                "book_display_on_website":                 "Books",
                "collectibles_display_on_website":         "Collectibles & Fine Art",
                "dvd_display_on_website":                  "Movies & TV",
                # "pc_display_on_website":                   "Computers & Accessories", sub-category of Electronics
                "fashion_display_on_website":              "Clothing, Shoes & Jewelry",
                "grocery_display_on_website":              "Grocery & Gourmet Food",
                "health_and_beauty_display_on_website":    "Health, Household & Baby Care",
                "home_garden_display_on_website":          "Home & Kitchen",
                # "furniture_display_on_website":            "Furniture", sub-category
                # "kitchen_display_on_website":              "Kitchen & Dining", sub-category
                "home_improvement_display_on_website":     "Tools & Home Improvement",
                "lawn_and_garden_display_on_website":      "Patio, Lawn & Garden",
                "luggage_display_on_website":              "Luggage & Travel Gear",
                "major_appliances_display_on_website":     "Appliances",
                "music_display_on_website":                "CDs & Vinyl",
                "musical_instruments_display_on_website":  "Musical Instruments",                
                "office_product_display_on_website":       "Office Products",
                "pantry_display_on_website":               "Prime Pantry",
                "pet_products_display_on_website":         "Pet Supplies",
                "photo_display_on_website":                "Camera & Photo",
                "software_display_on_website":             "Software",
                # "trading_cards_display_on_website":        "Trading Cards", sub-category of Sports Collectibles
                "sports_display_on_website":               "Sports & Outdoors",                
                "toy_display_on_website":                  "Toys & Games",
                "video_games_display_on_website":          "Video Games",
                "wireless_display_on_website":             "Cell Phones & Accessories"

                # "automotive_alt_display_on_website": "DUNNO",
                # "biss_basic_display_on_website": "DUNNO",
                # "biss_display_on_website": "DUNNO",
                # "boost_display_on_website": "DUNNOO",
                # "ce_display_on_website": "DUNNO",
                # "digital_music_album_display_on_website": "DUNNO",
                # "entmnt_collectibles_display_on_website": "DUNNO",
                # "sdp_misc_display_on_website": "DUNNO",
                # "target_outdoor_sport_display_on_website": "DUNNO",
                # "wir_phone_accessory_display_on_website": "DUNNO",
                }
    
    # Get catids and salesranks from Products_WmAz
    asinsStr = make_sql_list(asins, 'str')  # <('asin1', 'asin2', 'asin3', 'asin4')>
    
    con = con_postgres()
    sqlTxt = '''SELECT asin, salesrank1, catid1, salesrank2, catid2, salesrank3, catid3, salesrank4, catid4
                FROM "{}"
                WHERE asin in {}
                AND catid1 IS NOT NULL'''.format(theTable, asinsStr)        
    datums = call_sql(con, sqlTxt, [], "executeReturn", dictCur=True)
    
    catOrder = ({'salesrank': 'salesrank1', 'catid': 'catid1'},
                {'salesrank': 'salesrank2', 'catid': 'catid2'},
                {'salesrank': 'salesrank3', 'catid': 'catid3'},
                {'salesrank': 'salesrank4', 'catid': 'catid4'})
    
    # Match each item to its corresponding department using deptDict
    for item in datums:
        for cat in catOrder:
            checkCat = item[cat['catid']]
            item['dept'] = None
            if checkCat in deptDict:
                item['dept'] = deptDict[checkCat]
                item['rank'] = item[cat['salesrank']]
                break

    sqlTxt = '''SELECT dept_name, num_products 
                FROM "Az_Depts"'''                
    b = call_sql(con, sqlTxt, [], 'executeReturn', dictCur=True)
    numProdsLookup = {x['dept_name']: x['num_products'] for x in b}  # Convert tuple of dicts into a simple dict
    
    # Take the needed final values out of datums and put into theData, including calculated salesrank%
    theData = []
    for item in datums:
        if not item['dept']:
            salesrank = None
        else:
            try:
                salesrank = item['rank'] / numProdsLookup[item['dept']]
            except KeyError:
                if item['dept'] == 'Prime Pantry':
                    salesrank = None
                else:
                    print('public.Az_Depts does not have an entry for the <{}> category as required by ASIN {}'
                          .format(numProdsLookup[item['dept']], item['asin']))
                    salesrank = None
                
        theData.append([item['dept'],                     
                       salesrank,
                       item['asin']])
    
    sqlTxt = '''UPDATE "Products_WmAz"
                SET dept = %s, salesrank = %s
                WHERE asin = %s'''
    
    # Added this because it seemed like this SQL statement would continue running even after this function was done,
    # causing deadlocks with calc_column('net')'s SQL statement, which I don't know how to execute in ASIN order.
    for chunk in chunks(theData, 1000):
        call_sql(con, sqlTxt, chunk, 'executeBatch')
    
    if con:
        con.close()
        

def transfer_wm_datums(wmIds=[]):
    """
    Copy price, free_ship, and instock from Prod_Wm to Products_WmAz.
    If <wmIds> is empty, provide a list of wm_ids to transfer data for.
    """
    
    print('transfer_wm_datums() - starting...')
    
    con = con_postgres()
    
    sqlTxt = '''UPDATE "Products_WmAz" AS a
                SET wm_price = b.price,
                  free_ship =
                    CASE WHEN LOWER(b.free_ship) = 'true' then True
                    WHEN LOWER(b.free_ship) = 'false' then False
                    ELSE Null
                    END,
                  wm_instock =
                    CASE WHEN LOWER(b.in_stock) = 'available' then True
                    WHEN LOWER(b.in_stock) = 'not available' then False
                    Else Null
                    END
                FROM "Prod_Wm" AS b
                WHERE a.wm_id = b.wm_id'''
    if wmIds:
        sqlTxt += " AND a.wm_id IN ({})".format(",".join([str(i) for i in wmIds]))
    call_sql(con, sqlTxt, [], "executeNoReturn")
    
    if con:
        con.close()
    
    print('transfer_wm_datums() - finishing')
        
    
def calc_column(column, asins=None):
    """
    Fill out columns that don't require any kind of api call
    """
    
    print('Starting calc_column("{}")...'.format(column))
            
    con = con_postgres()
    
    if column == 'my_price':
        if asins:
            myPrices = get_my_price(asins)
        else:
            myPrices = get_my_price(get_all_asins())
            
        sqlTxt = '''UPDATE "Products_WmAz"
                    SET my_price = %s
                    WHERE asin = %s'''
        # Get rid of asins that don't have a my_price associated with them
        theData = tuple((myPrices[q], q,) for q in myPrices if myPrices[q])
        call_sql(con, sqlTxt, theData, 'executeBatch')
    
    if column == 'net':
        sqlTxt = '''UPDATE "Products_WmAz"
                    SET net = 
                    CASE WHEN fees_est = -1.00 THEN Null
                    WHEN my_price IS NOT Null THEN my_price - wm_price - fees_est
                    ELSE Null
                    END'''
        if asins:
            sqlTxt += ' WHERE asin IN {}'.format(make_sql_list(asins, 'str'))
        call_sql(con, sqlTxt, [], 'executeNoReturn')
    
    if column == 'salesrank':
        if asins:
            calc_sales_rank(asins)
        else:
            calc_sales_rank(get_all_asins())

    if con:
        con.close()
    
    print('calc_column("{}") has finished'.format(column))
        

def get_all_asins():
    """
    Returns all ASINs in Products_WmAz as a list
    """
    
    con = con_postgres()
    sqlTxt = '''SELECT asin 
                FROM "Products_WmAz"'''
    a = call_sql(con, sqlTxt, [], 'executeReturn')
    return [q[0] for q in a]
