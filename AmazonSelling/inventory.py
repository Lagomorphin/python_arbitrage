from AmazonSelling.routine import RoutineInventory
from AmazonSelling.tools import call_sql, record_timestamps, con_postgres
from Amazon.mwsutils import transfer_wm_datums, calc_column
from Walmart.walmartclasses import Lookup
import datetime


def update_skus():  # Under construction
    """
    Insert products into io.SKUs from io.Purchased, if io.SKUs doesn't have them yet
    Update the enroute, stock, processing, available, unit_profit, monthly_sales, and competition columns in io.SKUs
    """
    
    con = con_postgres()
    
    # Insert products from io.Purchased
    sqlTxt = '''INSERT INTO io."SKUs" (sku, asin, wm_id, wm_name, az_name)
                   SELECT DISTINCT ON (sku) sku, asin, wm_id, wm_name, az_name
                   FROM "Purchased"
                   --WHERE (LOWER(notes) NOT SIMILAR TO '%%(hazmat|restricted|never received|wrong asin)%%' OR notes IS Null)
                   WHERE omit_from_skus IS NOT True
                   ORDER BY sku
                ON CONFLICT ("sku") DO NOTHING'''  # %%'s are to escape the %'s
    call_sql(con, sqlTxt, [], "executeNoReturn")
    
    # enroute and stock columns
    # Why don't I just have it get data from the enroute and in_stock views?
    sqlTxt = '''UPDATE io."SKUs" AS a
                SET enroute = agg1.enroute_sum, stock = agg2.stock_sum
                FROM io."SKUs" b
                INNER JOIN (
                   SELECT sku, GREATEST(0, SUM(COALESCE(bought, 0) - COALESCE(received, 0) - COALESCE(cancelled, 0))) AS enroute_sum
                   FROM io."Purchased"
                   GROUP BY sku) AS agg1
                ON agg1.sku = b.sku
                INNER JOIN (
                   SELECT sku, GREATEST(0, SUM(COALESCE(received, 0) - COALESCE(packed, 0) - COALESCE(returned, 0) - COALESCE(lost, 0) - COALESCE(diff_channel, 0))) AS stock_sum
                   FROM io."Purchased"
                   GROUP BY sku) AS agg2
                ON agg2.sku = b.sku
                WHERE a.sku = b.sku'''
    call_sql(con, sqlTxt, [], "executeNoReturn")    
    
    # unit_profit, monthly_sales, and competition columns
    
    if con:
        con.close()
        
        
def update_already():
    """
    Retrieve data for already bought items from the Walmart API and MWS
    """
    
    con = con_postgres()
    wmIds = tuple(q[0] for q in call_sql(con, 'SELECT wm_id FROM io."SKUs" WHERE discontinue IS NOT True', [],
                                         'executeReturn'))
    asins = tuple(q[0] for q in call_sql(con, 'SELECT asin FROM io."SKUs" WHERE discontinue IS NOT True', [],
                                         'executeReturn'))
    skus = tuple(q[0] for q in call_sql(con, 'SELECT sku FROM io."SKUs" WHERE discontinue IS NOT True', [],
                                        'executeReturn'))
    con.close()
    
    update_already_wm(wmIds, asins)
    update_already_az(asins, skus)
    
    print('Done')
    return {'wmIds': wmIds, 'asins': asins, 'skus': skus}
        
    
def update_already_wm(wmIds, asins):
    """
    Retrieve data for already bought items from the Walmart API.
    Takes a list of wm_ids and a list of asins, corresponding to the items I've already bought at some point.
    Writes data to wm.Prod_Wm and Products_WmAz.
    """
    
    print('Updating Walmart data for {} already-bought items...'.format(len(wmIds)))
    
    wmLookup = Lookup()
    wmLookup.lookup_batch(wmIds)
     
    transfer_wm_datums(wmIds)  # Copy price, free_ship, and instock from Prod_Wm to Products_WmAz
    record_timestamps(asins, 'wm_data')
    
    
def update_already_az(asins, skus):
    """
    Retrieve data for already bought items from MWS.
    Takes a list of asins, corresponding to the items I've already bought at some point.
    Writes data to Products_WmAz.
    """
    
    e = RoutineInventory(asins, skus)
    e.routine()
    

if __name__ == '__main__':
    a = datetime.datetime.now()
    update_skus()
    itemsDict = update_already()
#     update_already_az(['B005IUJJLI', 'B01E4H7DTA'], ['B005IUJJLI-07.30.17', 'B01E4H7DTA-08.03.17'])
    calc_column('salesrank', asins=itemsDict['asins'])
    calc_column('net', asins=itemsDict['asins'])
    print("Total time elapsed: {}".format(str(datetime.datetime.now() - a).split('.')[0]))
