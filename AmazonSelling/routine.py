import abc
import datetime
import time
from collections import deque
from decimal import Decimal
from multiprocessing import Process, Pipe, Value, Lock
from multiprocessing.connection import wait

from Amazon.mwsutils import Products, FulfillmentInventory
from AmazonSelling.tools import call_sql, union_no_dups, make_sql_list, con_postgres
from Walmart.walmartclasses import WmRoutine, update_wm_data_timestamps


class Routine:
    
    __metaclass__ = abc.ABCMeta

    qDefs = {}
    triggers = {}
    
    throt = {
    'lmp':       {'maxreqquota': 20, 'restorerate': Decimal('0.2'), 'maxpercall': 1,  'minpercall': 1},
    'gpcfAsin':  {'maxreqquota': 20, 'restorerate': Decimal('0.2'), 'maxpercall': 1,  'minpercall': 1},
    'gmp':       {'maxreqquota': 20, 'restorerate': 2,              'maxpercall': 10, 'minpercall': 8},
    'gmpfId':    {'maxreqquota': 20, 'restorerate': 5,              'maxpercall': 5,  'minpercall': 4},
    'gcpfAsin':  {'maxreqquota': 20, 'restorerate': 10,             'maxpercall': 20, 'minpercall': 16},
    'glolfAsin': {'maxreqquota': 20, 'restorerate': 10,             'maxpercall': 20, 'minpercall': 16},
    'glpofAsin': {'maxreqquota': 10, 'restorerate': 5,              'maxpercall': 1,  'minpercall': 1},
    'gmfe':      {'maxreqquota': 20, 'restorerate': 10,             'maxpercall': 4,  'minpercall': 4},  # Should be 20
    'gmpfAsin':  {'maxreqquota': 20, 'restorerate': 10,             'maxpercall': 20, 'minpercall': 16},
    'lis':       {'maxreqquota': 30, 'restorerate': 2,              'maxpercall': 10, 'minpercall': 0}}  # Actual maxpercall is 50

    def __init__(self):
        pass
        
    def routine(self):
        
        mwsProducts = Products()
        procs = {
        'wm':        {'func': None,                                             'target': WmRoutine().routine},
        'lmp':       {'func': None,                                             'target': self.mws_proc},
        'gpcfAsin':  {'func': None,                                             'target': self.mws_proc},
        'gmp':       {'func': None,                                             'target': self.mws_proc},
        'gmpfId':    {'func': mwsProducts.match_to_az,                          'target': self.mws_proc},
        'gcpfAsin':  {'func': mwsProducts.get_comp_pricing,                     'target': self.mws_proc},
        'glolfAsin': {'func': mwsProducts.get_lowest_offer_listings,            'target': self.mws_proc},
        'glpofAsin': {'func': None,                                             'target': self.mws_proc},
        'gmfe':      {'func': mwsProducts.get_fees_est,                         'target': self.mws_proc},
        'gmpfAsin':  {'func': None,                                             'target': self.mws_proc},
        'lis':       {'func': FulfillmentInventory().get_list_inventory_supply, 'target': self.mws_proc}}
        
        ammo = {op: {'val': Value('i', self.throt[op]['maxreqquota']), 'lock': Lock()} for op in self.throt}
        
        # Holds the pipe ends that are passed to each process, and are used to signal when each process has finished.
        # Defines the hierarchy of the processes - the order in which they will terminate, from upstream to downstream.
        # The keys in <self.triggers> also define which procs will be run by this routine
                
        allDone = Value('i', 0)  # This is for throttle_refresher
        allDoneLock = Lock()
        
        thr = Process(target=self.throttle_refresher, args=(ammo, allDone))
        thr.start()
        
        allProcs = []
        self.qDefs = self.get_query_defs()
        update_wm_data_timestamps()
        
        for op in self.triggers:
            for sendEnd in self.triggers[op]['send']:
                self.triggers[sendEnd]['recv'][op], self.triggers[op]['send'][sendEnd] = Pipe(duplex=False)
                
        for op in self.triggers:
            theKwargs = {'op': op, 'func': procs[op]['func'], 'triggs': self.triggers[op]}
            if op in ammo:
                theKwargs['ammo'] = ammo[op]

            # noinspection PyTypeChecker
            procs[op]['proc'] = Process(target=procs[op]['target'], kwargs=theKwargs)
            procs[op]['proc'].start()
            
            for sendEnd in self.triggers[op]['send']:
                self.triggers[op]['send'][sendEnd].close()

            allProcs.append(procs[op]['proc'])
            
            '''
            Create another process that periodically prints out which processes are currently active
            It can use <allDone> to know when to stop
            '''
            # printr = Process(target=self.print_active_procs)
            # printr.start()

        for p in allProcs:
            p.join()
        
        with allDoneLock:
            allDone.value = 1
        
        thr.join()
    
    def mws_proc(self, **kwargs):
        """
        Continuously runs its MWS function until the previous MWS function reports that it's all done.
        When the previous MWS function (or Walmart's Routine, in the case of gmpfId) is done, this function
        will update its queue with the rest of the items, run through all those until they're complete, and
        then report itself as all done.
        """

        for key, value in kwargs.items():
            if key == 'op':
                op = value
            if key == 'func':
                func = value
            if key == 'ammo':
                ammo = value
            if key == 'triggs':
                triggs = value
             
#         if op == 'gmpfId':
#             print ("Asd")
#         else:
#             time.sleep(999999)
            
        if type(self.qDefs[op]['qry']) is str:
            isQry = True
            q = deque()
            
            # A dict with keys equal to the keys in triggs['recv'], and all values False
            finishUps = {recvEnd: False for recvEnd in triggs['recv']}
                    
        elif type(self.qDefs[op]['qry']) in (list, tuple):
            isQry = False
            q = deque(self.qDefs[op]['qry'])
            finishUps = {'bluhhuuuuurrrpp': True}
            
        else:
            print("Invalid value for self.qDefs[op]['qry'] in routine.Routine.mws_proc")
            return
        
        funcName = func.__name__

        throtGap = 1.1
        qGap = 8
        
        while True:
            prev = time.time()
            
            finished = all(q for q in finishUps.values())  # Will be T if all values in finishUps are T, else F
            
            # If we don't have enough items for a full call, check to see if more items are ready
            if len(q) < self.throt[op]['maxpercall']:
                if isQry:
                    q = self.fill_q(op, q)
                
                if finished and len(q) == 0:
                    print("{} signing off!".format(funcName))

                    for sendEnd in triggs['send']:
                        triggs['send'][sendEnd].send('')
                        triggs['send'][sendEnd].close()

                    break

            # Enough items in q to warrant an MWS call (which is anything > 0 if <finished> is True)
            if len(q) >= (1 - finished) * self.throt[op]['minpercall']:
                num = min(len(q), self.throt[op]['maxpercall'])
                if ammo['val'].value >= num:  # Not gonna get throttled
                    margs = [q.pop() for _ in range(num)]
                    
                    if margs:
                        if len(margs[0]) == 1:  # Convert a list of single-length tuples/lists to just a list
                            margs = [elem[0] for elem in margs]
                    
#                     Run the mwsutils function                    
                    print("{} - starting".format(funcName))
                    if op == 'gmpfId':
                        func('Walmart', 'upc', margs)
                    elif op == 'gcpfAsin':
                        func(margs)
                    elif op == 'glolfAsin':
                        func(margs)
                    elif op == 'gmfe':
                        func(margs)
                    elif op == 'lis':
                        func(margs)
                    print("{} - leaving".format(funcName))
                        
                    with ammo['lock']:
                        ammo['val'].value -= len(margs)
                        
                else:
                    time.sleep(max(prev + throtGap - time.time(), 0))
            else:
                # The only way I could find to trip a condition after a message is sent down a pipe in another process.
                # https://docs.python.org/3/library/multiprocessing.html#multiprocessing.connection.wait
                # Checks each receive Pipe end for a message. <finishedUps> records which receive Pipe ends have gotten
                # their message.
                for recvEnd in triggs['recv']:
                    if wait([triggs['recv'][recvEnd]], 0.1) and not finishUps[recvEnd]:
                        try:
                            _ = triggs['recv'][recvEnd].recv()
                            finishUps[recvEnd] = True
                        except EOFError:
                            print("{} giving EOFError".format(op))
                            finishUps[recvEnd] = True             
                
                time.sleep(max(prev + qGap - time.time(), 0))

    def fill_q(self, op, q):
        """
        Updates the queue for each MWS function from SQL
        """
        
        if 'args' not in self.qDefs[op]:
            self.qDefs[op]['args'] = []

        con = con_postgres()
        # Combine with existing queue, ensuring no duplicates
        blurp = deque(union_no_dups(list(q), call_sql(con, self.qDefs[op]['qry'], self.qDefs[op]['args'],
                                                      "executeReturn")))
        con.close()
            
        return blurp

    def throttle_refresher(self, ammo, allDone):
        """
        Keep tabs on MWS throttle values, and refresh them every second, or whatever <gap> is
        """
        
        gap = 1.05
        
        while not allDone.value:
            prev = time.time()
            for op in self.throt.keys():
                if ammo[op]['val'].value < self.throt[op]['maxreqquota']:
                    with ammo[op]['lock']:
                        ammo[op]['val'].value = min(ammo[op]['val'].value + self.throt[op]['restorerate'],
                                                    self.throt[op]['maxreqquota'])
#                     print ("{} - refreshed up to {}".format(op, ammo[op]['val'].value))
            time.sleep(max(prev + gap - time.time(), 0))
        
        print("throttle_refresher signing off!")

    @abc.abstractmethod
    def get_query_defs(self):
        pass
    
    def print_active_procs(self):
        pass
        raise NotImplementedError
    
    @abc.abstractmethod
    def define_attribs(self):
        raise NotImplementedError
        

class RoutineOGaster(Routine):
    
    triggers = {
    'wm':        {'recv': {},                                     'send': {'gmpfId':   None}},
    'gmpfId':    {'recv': {'wm':       None},                     'send': {'gcpfAsin': None,  'glolfAsin': None}},
    'gcpfAsin':  {'recv': {'gmpfId':   None},                     'send': {'gmfe':     None}},
    'glolfAsin': {'recv': {'gmpfId':   None},                     'send': {'gmfe':     None}},
    'gmfe':      {'recv': {'gcpfAsin': None,  'glolfAsin': None}, 'send': {}}}

    def define_attribs(self):
        raise NotImplementedError
    
    def get_query_defs(self):
        
        fillQDefs = {theProc: {} for theProc in ('gmpfId', 'gcpfAsin', 'glolfAsin', 'gmfe')}
    
        # GetMatchingProductsForID
        fillQDefs['gmpfId']['qry'] =    '''SELECT wm_id
                                           FROM "Prod_Wm"
                                           WHERE  upc IS NOT Null AND dup IS False
                                           AND (last_matched IS Null OR EXTRACT(EPOCH FROM (localtimestamp -
                                           last_matched)/86400) > 30)'''  # last_matched is either Null or at least 1 month old
        
        # GetCompetitivePricingForASIN
        fillQDefs['gcpfAsin']['qry'] =  '''SELECT a.asin
                                           FROM "Products_WmAz" AS a
                                           INNER JOIN "Timestamps_WmAz" AS b
                                           ON a.asin = b.asin
                                           INNER JOIN "Prod_Wm" AS c
                                           ON a.wm_id = c.wm_id
                                           WHERE a.free_ship = True AND a.wm_instock = True -- AND a.salesrank1 IS NOT Null
                                           AND EXTRACT(EPOCH FROM (localtimestamp - c.fetched)/86400) < 4
                                           AND (b.az_comp_price IS Null OR EXTRACT(EPOCH FROM (localtimestamp - b.az_comp_price)/3600) > 42)'''
        
        # GetLowestOfferListingsForASIN
        fillQDefs['glolfAsin']['qry'] = '''SELECT a.asin
                                           FROM "Products_WmAz" AS a
                                           INNER JOIN "Timestamps_WmAz" AS b
                                           ON a.asin = b.asin
                                           INNER JOIN "Prod_Wm" AS c
                                           ON a.wm_id = c.wm_id
                                           WHERE a.free_ship = True AND a.wm_instock = True -- AND a.salesrank1 IS NOT Null
                                           AND EXTRACT(EPOCH FROM (localtimestamp - c.fetched)/86400) < 4
                                           AND (b.az_lowest_offer IS Null OR EXTRACT(EPOCH FROM (localtimestamp - b.az_lowest_offer)/3600) > 42)'''
        
        # GetMyFeesEstimate
        fillQDefs['gmfe']['qry'] =      '''SELECT a.asin
                                           FROM "Products_WmAz" AS a
                                           INNER JOIN "Timestamps_WmAz" AS b
                                           ON a.asin = b.asin
                                           WHERE COALESCE(a.comp_price, a.lowest_fba, a.lowest_merch) IS NOT Null
                                           AND COALESCE(b.az_comp_price, DATE '0001-01-01') > COALESCE(b.az_fees, DATE '0001-01-02')
                                           AND COALESCE(b.az_lowest_offer, DATE '0001-01-01') > COALESCE(b.az_fees, DATE '0001-01-02')'''
        
        return fillQDefs
    

class RoutineDisplay1(Routine):
    """
    Go through the asins in Display1 and update their pricing and fees data
    This is to fix a problem will some products appearing to not returning accurate MWS data for one check.
    By checking again, those problem children will probably be removed from Display1 and save time when filling out
    manualSh.
    """
    
    triggers = {
    'gcpfAsin':  {'recv': {},                                    'send': {'gmfe': None}},
    'glolfAsin': {'recv': {},                                    'send': {'gmfe': None}},
    'gmfe':      {'recv': {'gcpfAsin': None, 'glolfAsin': None}, 'send': {}}}

    def define_attribs(self):
        raise NotImplementedError
    
    def get_query_defs(self):
        startTs = datetime.datetime.now()
        
        fillQDefs = {theProc: {} for theProc in ('gcpfAsin', 'glolfAsin', 'gmfe')}
        
        # GetCompetitivePricingForASIN
        fillQDefs['gcpfAsin']['qry'] =  '''SELECT a.asin
                                           FROM "Display1" AS a
                                           INNER JOIN "Timestamps_WmAz" AS b
                                           ON a.asin = b.asin
                                           INNER JOIN "Products_WmAz" AS c
                                           ON a.asin = c.asin
                                           WHERE %s > b.az_comp_price
                                           --AND (c.comp_price IS NOT Null AND c.lowest_fba IS Null)'''
        
        # GetLowestOfferListingsForASIN
        fillQDefs['glolfAsin']['qry'] = '''SELECT a.asin
                                           FROM "Display1" AS a
                                           INNER JOIN "Timestamps_WmAz" AS b
                                           ON a.asin = b.asin
                                           INNER JOIN "Products_WmAz" AS c
                                           ON a.asin = c.asin
                                           WHERE %s > b.az_lowest_offer
                                           --AND (c.comp_price IS NOT Null AND c.lowest_fba IS Null)'''
        
        # GetMyFeesEstimate
        fillQDefs['gmfe']['qry'] =      '''SELECT a.asin
                                           FROM "Display1" AS a
                                           INNER JOIN "Timestamps_WmAz" AS b
                                           ON a.asin = b.asin
                                           WHERE b.az_comp_price > %s
                                           AND b.az_lowest_offer > %s
                                           AND b.az_fees < %s'''
        
        for g in fillQDefs:
            if g == 'gcpfAsin':
                fillQDefs[g]['args'] = [startTs]
            if g == 'glolfAsin':
                fillQDefs[g]['args'] = [startTs]
            if g == 'gmfe':
                fillQDefs[g]['args'] = [startTs, startTs, startTs]
            
        return fillQDefs
    
    
class RoutineInventory(Routine):
    
    triggers = {
    'gcpfAsin':  {'recv': {},                                    'send': {'gmfe': None}},
    'glolfAsin': {'recv': {},                                    'send': {'gmfe': None}},
    'gmfe':      {'recv': {'gcpfAsin': None, 'glolfAsin': None}, 'send': {}},
    'lis':       {'recv': {},                                    'send': {}}}

    def define_attribs(self):
        raise NotImplementedError
    
    def __init__(self, asins, skus):  # This one needs an __init__ since it takes parameters
        super().__init__()  # Not sure if this is really needed, or just satisfies PyCharm
        self.asins = asins
        self.skus = skus
    
    def get_query_defs(self):
        
        startTs = datetime.datetime.now()
        
        fillQDefs = {theProc: {} for theProc in ('gcpfAsin', 'glolfAsin', 'gmfe', 'lis')}
        
        # GetCompetitivePricingForASIN
        fillQDefs['gcpfAsin']['qry'] = self.asins
        
        # GetLowestOfferListingsForASIN
        fillQDefs['glolfAsin']['qry'] = self.asins
        
        # GetMyFeesEstimate
        fillQDefs['gmfe']['qry'] = '''SELECT a.asin
                                      FROM "SKUs" AS a
                                      INNER JOIN "Timestamps_WmAz" AS b
                                      ON a.asin = b.asin
                                      WHERE b.az_comp_price > %s
                                      AND b.az_lowest_offer > %s
                                      AND b.az_fees < %s'''  
        
        # ListInventorySupply
        fillQDefs['lis']['qry'] = self.skus
    
        fillQDefs['gmfe']['args'] = [startTs, startTs, startTs]
        
        return fillQDefs


class RoutineManually(Routine):
    """
    Add items to Prod_Wm, Products_WmAz, and Timestamps_WmAz manually from wm_ids
    """
    
    triggers = {
    'gmpfId':    {'recv': {},                                     'send': {'gcpfAsin': None,  'glolfAsin': None}},
    'gcpfAsin':  {'recv': {'gmpfId':   None},                     'send': {'gmfe':     None}},
    'glolfAsin': {'recv': {'gmpfId':   None},                     'send': {'gmfe':     None}},
    'gmfe':      {'recv': {'gcpfAsin': None,  'glolfAsin': None}, 'send': {}}}

    def define_attribs(self):
        raise NotImplementedError
    
    def __init__(self, wmIds):  # This one needs an __init__ since it takes parameters
        super().__init__()  # Not sure if this is really needed, or just satisfies PyCharm
        self.wmIds = wmIds
    
    def get_query_defs(self):
        
        startTs = datetime.datetime.now()
        
        fillQDefs = {theProc: {} for theProc in ('gmpfId', 'gcpfAsin', 'glolfAsin', 'gmfe')}
        wmIdsSQLList = make_sql_list(tuple(t[0] for t in self.wmIds), 'int')
    
        # GetMatchingProductsForID
        fillQDefs['gmpfId']['qry'] = self.wmIds
        
        # GetCompetitivePricingForASIN
        fillQDefs['gcpfAsin']['qry'] =  '''SELECT a.asin
                                           FROM "Products_WmAz" AS a
                                           INNER JOIN "Timestamps_WmAz" AS b
                                           ON a.asin = b.asin
                                           WHERE a.wm_id IN {}
                                           AND COALESCE(b.match_to_az, DATE '0001-01-01') >= %s
                                           AND (b.az_comp_price IS Null OR b.az_comp_price < %s)'''.format(wmIdsSQLList)
        
        # GetLowestOfferListingsForASIN
        fillQDefs['glolfAsin']['qry'] = '''SELECT a.asin
                                           FROM "Products_WmAz" AS a
                                           INNER JOIN "Timestamps_WmAz" AS b
                                           ON a.asin = b.asin
                                           WHERE a.wm_id IN {}
                                           AND COALESCE(b.match_to_az, DATE '0001-01-01') >= %s
                                           AND (b.az_lowest_offer IS Null OR b.az_lowest_offer < %s)'''.format(wmIdsSQLList)
        
        # GetMyFeesEstimate
        fillQDefs['gmfe']['qry'] =      '''SELECT a.asin
                                           FROM "Products_WmAz" AS a
                                           INNER JOIN "Timestamps_WmAz" AS b
                                           ON a.asin = b.asin
                                           WHERE a.wm_id IN {}
                                           AND b.az_comp_price > %s
                                           AND b.az_lowest_offer > %s
                                           AND (b.az_fees IS Null OR b.az_fees < %s)'''.format(wmIdsSQLList)
        
        fillQDefs['gcpfAsin']['args'] = [startTs, startTs]
        fillQDefs['glolfAsin']['args'] = [startTs, startTs]                                  
        fillQDefs['gmfe']['args'] = [startTs, startTs, startTs]
        
        return fillQDefs
