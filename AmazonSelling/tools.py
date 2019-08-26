from operator import itemgetter
from random import randint

import datetime
import dateutil.parser
import inspect
import os
import psycopg2.extras
import requests
import configparser


def get_request(url, theTimeout, retries):
    """
    returns a library containing the result of the query and the number of required attempts
    'result' entry of returned library is None if all attempts failed
    If theTimeout is -1, timeout will start at 1 sec and increase by 1 every time the request fails
    http://stackoverflow.com/questions/21371809/cleanly-setting-max-retries-on-python-requests-get-or-post-method?rq=1
    """
    
    connErr = False
    g = theTimeout
    i = 1
    while i <= retries:
        if theTimeout == -1:
            g = max(g + 1, 1)
            
        try:
            datums = {'result': requests.get(url, timeout=g), 'numTries': i}
        except requests.ConnectionError:
            connErr = True
            i += 1
        except Exception:
            connErr = False
            i += 1
            print('timeout: ', g)
        else:
            connErr = False
            return datums
    
    if connErr:
        print('ConnectionError ({})'.format(datetime.datetime.now().strftime('%m/%d/%Y %I:%M:%S %p')))
    return {'result': None, 'numTries': i - 1}


def get_credentials(defs=None, rType=dict):
    """
    Used to get various credentials out of credentials.ini

    Parameters: defs, rType
    Returns: credentials extracted from credential.ini
    """

    conf = configparser.ConfigParser()
    conf.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.path.pardir, 'credentials.ini'))

    if not defs:  # Return a dict of dicts containing the entire contents of the config file, even if <rType> is 'tuple'
        return {sect: {opt: conf.get(sect, opt) for opt in conf.options(sect)} for sect in conf.sections()}
    else:  # Return the requested values
        # First, convert any single options into tuples
        fixedDefs = {sect: (defs[sect],) if type(defs[sect]) not in (list, tuple) else defs[sect] for sect in defs.keys()}

        # If <defs> only contains one requested value, simply return that value
        if len(fixedDefs) == 1 and len(fixedDefs[list(fixedDefs)[0]]) == 1:
            return conf.get(list(fixedDefs)[0], fixedDefs[list(fixedDefs)[0]][0])
        else:
            if rType == list:
                print("rType of 'list' is not yet implemented")
            elif rType == dict:
                # Return a dict of dicts containing the selected contents of the config file
                return {sect: {opt: conf.get(sect, opt) for opt in fixedDefs[sect]} for sect in fixedDefs.keys()}
            else:
                raise ValueError('<rType> should be either list or dict')
    

def datetime_floor(minuteInterval, theTs=None):
    """
    Rounds a datetime object down to nearest minuteInterval.
    If <theTs> is omitted, will use current datetime. Otherwise, theTs should contain a datetime.
    """
    
    if not theTs:
        theTs = datetime.datetime.now()
    
    if theTs:
        if minuteInterval < 1.0/60:
            return theTs - datetime.timedelta(microseconds=theTs.microsecond % (minuteInterval * 60.0 * 1000000.0))
        elif minuteInterval < 1:
            return theTs - datetime.timedelta(seconds=theTs.second % (minuteInterval * 60.0), microseconds=theTs.microsecond)
        else:
            return theTs - datetime.timedelta(minutes=theTs.minute % minuteInterval, seconds=theTs.second, microseconds=theTs.microsecond)


def con_postgres():
    """
    :return: Postgres connection object to the database defined in credentials.ini
    """
    credParts = ('dbname', 'user', 'password')
    creds = get_credentials({'PostgreSQL': credParts})['PostgreSQL']
    conStr = ' '.join(['{}={}'.format(credPart, creds[credPart]) for credPart in credParts if creds[credPart]])
    return psycopg2.connect(conStr)
    

def call_sql(con, sqlTxt, theData, qryType, dictCur=False):
    
    err = False
    
    if dictCur:
        cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    else:
        cur = con.cursor()
    
    try:
        if qryType == 'executeNoReturn':
            cur.execute(sqlTxt, theData)
            con.commit()
        elif qryType == 'executeReturn':
            cur.execute(sqlTxt, theData)
            values = cur.fetchall()
        elif qryType == 'executeBatch':
            psycopg2.extras.execute_batch(cur, sqlTxt, theData)
            con.commit()
        else:
            print('qryType not recognized, sorry')
            err = True
            
    except psycopg2.DatabaseError as e:
        err = True
        if con:
            con.rollback()
        print('DatabaseError %s' % e)
        
    except KeyError as e:
        err = True
        print('KeyError: Dictionary key %s was not found in the set of existing keys' % e)
        
    if cur:
        cur.close()
    
    if qryType == 'executeReturn' and not err:
        return values
    

def write_to_file(filename, data, dirrr='', absPath=False):
    """
    If <dirrr> is ommited and <absPath> is False, file will be written to the same folder as the caller
    If <absPath> is False, then file will be written in a folder relative to the caller. <dirrr> defines this folder.
    """
    
    if absPath:
        _dirrr = dirrr
    else:  # We're going by relative path
        # Get the location of the caller function, and paste dirrr on the end to get the full path
        _dirrr = '{}/{}'.format(os.path.dirname(os.path.abspath(inspect.stack()[1][1])), dirrr)
    
    with open(os.path.join(_dirrr, filename), 'w', encoding='utf-8') as f:
        f.write(data)
    

def union_no_dups(j, k):
    """
    Returns a list of lists containing the union of j and k without duplicates
    Duplicates are defined by matching values in the first columns of j and k. For instance: If j[6][0] == k[52][0]
    j and k are lists of lists or lists of tuples.
    """
    
    # Convert j to list of lists
    if j:  # j is not empty
        if isinstance(j[0], tuple):
            j = [list(elem) for elem in j]
    
    if k:  # k is not empty
    
        # Make lists of the first columns in each list of lists, j and k
        m = [i[0] for i in j]
        n = [i[0] for i in k]
        
        # Make a list of all indices in n that do not have duplicate values in m
        nondupIndices = [ind[0] for ind in enumerate(n) if not n[ind[0]] in m]
        
        # Append all the non-dupes in k into j
        for q in nondupIndices:
            j.append(list(k[q]))
        return j
    
    else:
        return j
    

def record_timestamps(datums, col):
    # Log the timestamp of an operation (col)
    # <datums>can either be a list of asins, or a list of lists contains asins and timestamps
    
    if col in ['wm_data', 'match_to_az', 'az_comp_price', 'az_fees', 'az_lowest_offer']:
        tbl = 'Timestamps_WmAz'
    else:
        print('The function {} did not receive a suitable argument for <col>. What it got was: {}: {}'
              .format(record_timestamps.__name__, type(col), col))
        return    

    if isinstance(datums[0], list) or isinstance(datums[0], tuple):  # datums is 2D, which means it includes timestamps
        theData = [(i[0], datetime_floor(1.0/60, theTs=i[1]), datetime_floor(1.0/60, theTs=i[1]),) for i in datums]
    else:  # datums just has asins, no timestamps
        ts = datetime_floor(1.0/60)
        theData = [(i, ts, ts,) for i in datums]
        
    # Sort theData by the first value (ASIN) of each tuple. This is to prevent Postgres deadlocks --->
    # https://www.postgresql.org/docs/9.4/static/explicit-locking.html#LOCKING-DEADLOCKS

    # key=itemgetter(0) is faster than key=lambda tup: tup[0] --->
    # https://stackoverflow.com/questions/17243620/operator-itemgetter-or-lambda/17243726
    theData.sort(key=itemgetter(0))
    
    sqlTxt = '''INSERT INTO "{}" (asin, {})
                VALUES(%s, %s)
                ON CONFLICT ("asin") DO UPDATE
                SET {} = %s'''.format(tbl, col, col)
    con = con_postgres()
    call_sql(con, sqlTxt, theData, 'executeBatch')
    if con:
        con.close()
        

def str_to_datetime(theStr):
    """
    Converts a date(s) to  datetime object(s)
    Parameters: theStr (string or tuple/list of string)
    Returns: datetime object or tuple of datetime objects
    """
    
    if type(theStr) in (list, tuple):
        return [dateutil.parser.parse(x) for x in theStr]
    else:
        if not theStr:
            return None
        return dateutil.parser.parse(theStr)
    

def change_asin(old, new):
    """
    Changes an asin to a new one across all SQL tables containing asins
    For tables other than io.SKUs, the asin will only be changed if the new asin doesn't already exist in the table.
    """
    
    sqlTxt = '''UPDATE io."Manual"
                SET asin = %s
                WHERE asin = %s
                AND NOT EXISTS (SELECT 1 FROM io."Manual" WHERE asin = %s);
                
                UPDATE io."Purchased"
                SET asin = %s
                WHERE asin = %s
                AND NOT EXISTS (SELECT 1 FROM io."Purchased" WHERE asin = %s);
                
                UPDATE io."SKUs"
                SET asin = %s, notes = CONCAT_WS('. ', notes, 'asin changed from {} to {}')
                WHERE asin = %s;
                
                UPDATE "Products_WmAz"
                SET asin = %s
                WHERE asin = %s
                AND NOT EXISTS (SELECT 1 FROM "Products_WmAz" WHERE asin = %s);
                
                UPDATE "Timestamps_WmAz"
                SET asin = %s
                WHERE asin = %s
                AND NOT EXISTS (SELECT 1 FROM "Timestamps_WmAz" WHERE asin = %s);'''.format(old, new)
    theData = [new, old, new, new, old, new, new, old, new, old, new, new, old, new]
    con = con_postgres()
    call_sql(con, sqlTxt, theData, 'executeNoReturn')
    if con:
        con.close()
        
    
def recreate_table(sourceTable, colsTupl=None, pKey=None, newTblName=None):
    """
    Duplicates a table. Used to rearrange the columns.
    Creates a table with columns and rows from <sourceTable>. <sourceTable> should have the schema prefix.
    <colsTupl> is a tuple of the columns to include in the new table. If it's left as None, all columns in <sourceTable> will be used.
    <pKey> is the primary key. If it's None, the primary key from <sourceTable> will be used.
    If <newTblName> is None, the new table will be named after <sourceTable> be with a random number on the end.
    If <newTblName> has a schema prefix, it will be used. Else, the schema from <sourceTable> will be used, if it exists.
    """
    
    if sourceTable == newTblName:
        print("recreate_table: <sourceTable> and <newTblName> are the same, can't proceed.")
        return
    
    con = con_postgres()
        
    # Format the both table names to have the appropriate quotes depending on if it includes a schema or not
    # Construct <newTblName> if needed
    sYerp = sourceTable.split('.')
    if newTblName:
        nYerp = newTblName.split('.')
    else:
        nYerp = []
        
    if len(sYerp) == 2:
        sourceTblName = '.'.join([sYerp[0], '"{}"'.format(sYerp[1])])
        metaTblName = "'{}'".format(sYerp[1])
        pKeyTblName = '"{}"'.format(sYerp[1])
        
        if len(nYerp) == 1:
            newTblName = ".".join([sYerp[0], '"{}"'.format(nYerp[0])])
    else:
        sourceTblName = '"{}"'.format(sourceTable)
        metaTblName = "'{}'".format(sourceTable)
        pKeyTblName = '"{}"'.format(sourceTable)
        
        if len(nYerp) == 1:
            newTblName = '"{}"'.format(newTblName)
            
    if len(nYerp) == 2:
        newTblName = ".".join([nYerp[0], '"{}"'.format(nYerp[1])])
    if len(nYerp) == 0:
        newTblName = sourceTblName[:-1] + '_' + str(randint(0, 9999)) + '"'
        
    # Get the column names
    sqlTxt = '''SELECT column_name FROM information_schema.columns
                WHERE table_name = {}'''.format(metaTblName)
#     print(sqlTxt + '\n\n')
    sourceColsTupl = tuple(a[0] for a in call_sql(con, sqlTxt, [], 'executeReturn'))
    print("\n{}'s columns are:\n{}\n".format(sourceTable, sourceColsTupl))
    
    # Write out the columns list as a string
    if colsTupl:
        newColsStr = ', '.join(tuple(a for a in colsTupl if a in sourceColsTupl))
    else:
        newColsStr = ', '.join(sourceColsTupl)
    
    # Determine the primary key. If none is passed and <sourceTable> doesn't have one, then <pKey> will remain as None
    # https://wiki.postgresql.org/wiki/Retrieve_primary_key_columns
    if pKey:
        if pKey not in sourceColsTupl:
            print("recreate_table: the passed primary key '{}' can't be used because it doesn't exist in the"
                  "source table, '{}'.".format(pKey, sourceTable))
            return
    else:
        sqlTxt = '''SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS data_type
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = '{}'::regclass
                    AND i.indisprimary'''.format(pKeyTblName)
#         print(sqlTxt + '\n\n')
        maybePKey = call_sql(con, sqlTxt, [], "executeReturn")
        if maybePKey:
            pKey = maybePKey[0][0]
    
    # Create the new table
    sqlTxt = '''CREATE TABLE {0} AS
                   SELECT {1}
                   FROM {2};'''.format(newTblName, newColsStr, sourceTblName)                   
    if pKey:
        sqlTxt += ' ALTER TABLE {0} ADD PRIMARY KEY ({1});'.format(newTblName, pKey)
    print(sqlTxt)
        
    call_sql(con, sqlTxt, [], "executeNoReturn")    
    
    if con:
        con.close()
        
    
def chunks(theList, n):
    """
    Yields successive n-sized chunks from <theList>
    """
    
    for i in range(0, len(theList), n):
        yield theList[i:i + n]
        

def make_sql_list(theList, theType):
    """
    Create a string from a list or tuple to be used in a SQL query.
    Parameters: theList (list or tuple, 1-D), theType (string: 'int' or 'str')
    """
    
    if theType == 'int':
        return '({})'.format(','.join(map(str, theList)))
    elif theType == 'str':
        return "('{}')".format("','".join(theList))
    else:
        raise ValueError("<theType> should be a string with a value of either 'int' or 'str'")
