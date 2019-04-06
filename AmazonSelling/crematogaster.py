import datetime
import math
import time

from Amazon.mwsutils import calc_column, transfer_wm_datums
from AmazonSelling.routine import RoutineOGaster, RoutineDisplay1, RoutineManually
from AmazonSelling.tools import call_sql, con_postgres
from Walmart.walmartclasses import update_wm_data_timestamps, Lookup


def crematogaster():
    
    a = time.time()
    while True:
        b = time.time()
        
        lasius = RoutineOGaster()
        lasius.routine()
        
        update_wm_data_timestamps()
        calc_column('salesrank')
        time.sleep(60)
        calc_column('net')
        
        print('Time elapsed: {}'.format(time.strftime('%X', time.gmtime(time.time() - b))))
        print('Total time elapsed: {}'.format(time.strftime('%X', time.gmtime(time.time() - a))))
        napLength = math.ceil(time.time() / 3600.0) * 3600.0 - time.time()  # number of seconds until next hour begins
        print('Napping for {}, until {}'.format(time.strftime('%X', time.gmtime(napLength)), time.strftime('%X', time.localtime(time.time() + napLength))))
        time.sleep(napLength)
        
        
def prepare_display1():
    """
    Refresh Display1, then run routine.RoutineDisplay1 on the asins in Display1 to update their MWS data, then refresh Display1 again.
    This will usually filter out asins that only originally made it into Display1 due to some MWS data fluke.
    """
    a = datetime.datetime.now()
    
    transfer_wm_datums()
    calc_column('salesrank')
    calc_column('net')
    
    con = con_postgres()
    call_sql(con, 'REFRESH MATERIALIZED VIEW public."Display1"', [], "executeNoReturn")
    con.close()
      
    e = RoutineDisplay1()
    e.routine()
    
    calc_column('net')
    
    con = con_postgres()
    call_sql(con, 'REFRESH MATERIALIZED VIEW public."Display1"', [], "executeNoReturn")
    con.close()
    
    print('Total time elapsed: {}'.format(str(datetime.datetime.now() - a).split('.')[0]))
    
    
def add_items_manually(wmIds):
    """
    Add items to Prod_Wm, Products_WmAz, and Timestamps_WmAz manually from wm_ids
    Parameters: wmIds (list or tuple)
    """
    
    a = datetime.datetime.now()
     
    nestedWmIds = tuple([g] for g in wmIds)
     
    wmLookup = Lookup()
    wmLookup.lookup_batch(wmIds)
     
    e = RoutineManually(nestedWmIds)
    e.routine()
    
    calc_column('net')
    calc_column('salesrank')
     
    print('Total time elapsed: {}'.format(str(datetime.datetime.now() - a).split('.')[0]))
    

if __name__ == '__main__':
    
    crematogaster()
#     prepare_display1()
#     add_items_manually([53322425])
