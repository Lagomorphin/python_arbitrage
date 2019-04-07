import datetime
import itertools
import re
import xlwings as xw

from AmazonSelling.tools import call_sql, str_to_datetime, con_postgres


def sql_to_xl(tbl, wkbk=None, sh='Sheet1', where=None):
    """
    Writes a postgres table into Excel
    <where> allows a SQL WHERE clause to be added to the SELECT statement
    """
    
    if wkbk:
        sht = wkbk.sheets(sh)
    else:
        sht = xw.Book.caller().sheets(sh)
        
    fullRange = sht.range('A1').current_region
    headersRow = fullRange.rows(1)
    try:
        datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    except AssertionError:  # No data currently in the sheet
        datumsRange = headersRow.offset(1, 0)
    
    datumsRange.clear_contents()
    
    # Format the table name to have the appropriate quotes depending on if it includes a schema or not
    yerp = tbl.split('.')
    if len(yerp) == 2:
        theTbl = ".".join([yerp[0], '"{}"'.format(yerp[1])])
        metaTblName = "'{}'".format(yerp[1])
    else:
        theTbl = '"{}"'.format(tbl)
        metaTblName = "'{}'".format(tbl)
        
    # Get the column names from SQL.
    # This method is used instead of information_schema because it works for materialized views.
    sqlTxt = '''SELECT a.attname
                FROM pg_attribute a
                  JOIN pg_class t ON a.attrelid = t.oid
                WHERE a.attnum > 0 AND NOT a.attisdropped AND t.relname = {}'''.format(metaTblName)
    con = con_postgres()
    sqlTblNames = tuple(i[0] for i in call_sql(con, sqlTxt, [], 'executeReturn'))
        
    # Get column names from Excel, using the headers that have comments.
    # If the excel header exists in the SQL table, append the header as-is
    # If the excel header doesn't exist in the SQL table, still append it, but precluded with "Null AS".
    colNames = []
    colIndexes = []
    for header in headersRow:
        try:
            _ = header.api.Comment.Text()
        except:
            print("Not using {}".format(header.value))
        else:
            if header.value in sqlTblNames:
                colNames.append(header.value)
            else:
                colNames.append('Null AS {}'.format(header.value))
            colIndexes.append(header.column)
            
    colNamesStr = ", ".join(colNames)
    
    sqlTxt = '''SELECT {}
                FROM {}'''.format(colNamesStr, theTbl)
    if where:
        sqlTxt += ' {}'.format(where)
    
    print(sqlTxt)    
    
    # Double tuple comprehension: creates a tuple of tuples with the column index and the data for that column.
    # The data part of each 2nd level tuple is itself a tuple, and transposed so the data pastes in as columns instead
    # of rows.
    datumsByCol = tuple((colIndexes[i], tuple((j[i],) for j in call_sql(con, sqlTxt, [], 'executeReturn')),)
                        for i in range(len(colIndexes)))
    if con:
        con.close()
    
    for col in datumsByCol:
        datumsRange(1, col[0]).value = col[1]

    
def xl_to_sql(tbl, caller, sh='Sheet1', upsert=False):
    """
    Writes the data from the Excel sheet <sh> in workbook <caller> into the SQL table <tbl>.
    If <tbl> already exists, the rows will be inserted/upserted. Otherwise, a new table will be created.
    If <upsert> is True, the rows will be upserted. Otherwise, they'll be inserted.
    Only columns with comments in the headers will be written to SQL. If a new table is being created, its
    columns' types will be based on the text of the comments.
    The first column of the Excel sheet must be the primary key of the SQL table.
    """
    
    con = con_postgres()
    
    # Format the table name to have the appropriate quotes depending on if it includes a schema or not
    yerp = tbl.split(".")
    if len(yerp) == 2:
        tbl = '.'.join([yerp[0], '"{}"'.format(yerp[1])])
    else:
        tbl = '"{}"'.format(tbl)
    
    sht = caller.sheets(sh)
    
    a = sht.range('A1').current_region
    numItems = a.rows.count - 1
    
    # Get column names and types from Excel
    colNames, colTypes, datums = ([] for _ in range(3))
    colNum = 1
    for i in a.resize(1):
        try:
            colTypes.append(i.api.Comment.Text())
        except:
            print('Not using {}'.format(i.value))
        else:
            colNames.append(i.value)
            datums.append(a.resize(numItems, 1).offset(1, colNum - 1).value)
        finally:
            colNum += 1
    
    # If datums isn't 2d (only 1 item was appended) then make it 2d anyways
    if not isinstance(datums[0], list):
        datums = [[vvv] for vvv in datums]
    
    # Transpose datums. https://stackoverflow.com/questions/6473679/transpose-list-of-lists
    datums = list(map(list, itertools.zip_longest(*datums)))
    
    # Re-add non-primary key columns to datums for upsert (These columns need to be included twice)
    # ['Books', 62428868] becomes ['Books', 62428868, 62428868] for each inner list in datums, which is a list of lists
    if upsert:
        for item in datums:
            item.extend(item[1:len(item)])
    
    # Write out the <column_name>, <column_type> part of the CREATE TABLE statement
    if colTypes:
        if colTypes[0] not in ['', '_', 'Tim:_']:
            nameType = []
            for i in range(0, len(colNames)):
                if i == 0:  # First column is automatically assigned as PRIMARY KEY
                    nameType.append('{} {} NOT NULL PRIMARY KEY'.format(colNames[i], colTypes[i]))
                else:
                    nameType.append('{} {}'.format(colNames[i], colTypes[i]))
            nameTypeTxt = ', '.join(nameType)
            
            # Create a table with the correct column names and types
            sqlTxt = '''CREATE TABLE IF NOT EXISTS {} ({});'''.format(tbl, nameTypeTxt)
            call_sql(con, sqlTxt, [], 'executeNoReturn')
    
    names = ", ".join(colNames)
    formatters = ", ".join(['%s'] * len(colNames))
    
    if upsert:
        afterSetList = []
        for i in range(1, len(colNames)):  # Skip first colName, which is the primary key
            afterSetList.append('{} = %s'.format(colNames[i]))
        afterSet = ", ".join(afterSetList)
        
        # Insert the data into the table
        sqlTxt = '''INSERT INTO {} ({})
                    VALUES({})
                    ON CONFLICT ("{}") DO UPDATE
                    SET {}'''.format(tbl, names, formatters, colNames[0], afterSet)
    else:
        # Insert the data into the table
        sqlTxt = '''INSERT INTO {} ({})
                    VALUES({})'''.format(tbl, names, formatters)
        
    print(sqlTxt)
    call_sql(con, sqlTxt, datums, 'executeBatch')
    
    if con:
        con.close()
        

def test_get_arrays():
    enrouteSh = xw.Book.caller().sheets('Enroute')
    
    fullRange = enrouteSh.range("A1").current_region
    _headersRow = fullRange.rows(1)
    datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    expireCol = datumsRange.columns(_headersRow.value.index('expire') + 1)
    purchaseIdCol = datumsRange.columns(_headersRow.value.index('purchase_id') + 1)
    
    qwe = get_arrays_list(enrouteSh, expireCol, (datetime.datetime, int))
    
    theData = [[qwe[i], purchaseIdCol[i].value] for i, _ in enumerate(qwe)]
     
    sqlTxt = '''UPDATE io."Purchased"
                SET expire = %s
                WHERE purchase_id = %s'''
    con = con_postgres()
    call_sql(con, sqlTxt, theData, 'executeBatch')
    con.close()

        
def test_insert_lists():
    enrouteSh = xw.Book.caller().sheets('Enroute')
    
    fullRange = enrouteSh.range('A1').current_region
    _headersRow = fullRange.rows(1)
    datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    purchaseIdCol = datumsRange.columns(_headersRow.value.index('purchase_id') + 1)
    
    # SELECTs from multiple parameters in list form (purchaseIdCol.value)
    # The return is ordered the same as the parameter list was ordered
    # https://stackoverflow.com/a/35456954/5253431
    sqlTxt = '''SELECT expire
                FROM "Purchased"
                JOIN UNNEST('{{{}}}'::text[]) WITH ORDINALITY t(purchase_id, ord) USING (purchase_id)
                ORDER BY t.ord'''.format(','.join(purchaseIdCol.value))
                
    con = con_postgres()
    expire = tuple(q[0] for q in call_sql(con, sqlTxt, purchaseIdCol.value, 'executeReturn'))
    con.close()
    
    insert_lists(expire, xw.Book.caller(), 'notes', 'Enroute')


#############
### Tools ###
#############

def insert_lists(datums, col, sh, wkbk=None):

    if wkbk:
        sht = wkbk.sheets(sh)
    else:
        sht = xw.Book.caller().sheets(sh)
        
    fullRange = sht.range('A1').current_region
    _headersRow = fullRange.rows(1)
    datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    
    if len(datums) != datumsRange.rows.count:
        raise RuntimeError('xlfuncs.insert_lists: The length of the <datums> parameter is different from the number of '
                           'data rows in the Excel sheet <{}>'.format(sh))
    
    for i, _ in enumerate(datumsRange.rows):
        theCell = datumsRange.columns(_headersRow.value.index(col) + 1)(i + 1)
        
        if datums[i]:
            theCell.api.Validation.delete()
            listStr = ''
            
            listStr += ', '.join([':'.join(listVal) for listVal in datums[i]])
            
            theCell.api.Validation.add(3, 1, 3, listStr)
            d = ':'.join(datums[i][0])
            theCell.formula = d
            
        else:
            theCell.formula = False
    

def get_arrays_list(sh, rng, dimTypes):  # sh is only needed for error messages
    """
    Gets and formats the data in range <rng> into a format used for a postgres array type.
    Each cell must either be blank, contain a date with count value, or contain a data validation list of dates and count values.
    Dates and count values must be separated by a colon.
    
    Parameters: sh, rng, dimTypes (a list/tuple of the Python data types of each dimension the array elements)
    Returns: a list of lists representing the data from the entire column. Each inner list contains 1 postgres array.
    """
    
    if type(rng) is xw.main.Range:
        # Extract the data validation list from each cell
        datums = []
        for theCell in rng:
            try:
                datums.append(theCell.api.Validation.Formula1)
            except:
                # If there's no data validation list, get whatever's in the cell anyways in case it's an exp. date
                v = theCell.value
                if v:
                    w = v.strip()
                    if w:
                        datums.append(v)
                    else:
                        datums.append(None)
                else:
                    datums.append(None)
                
    elif type(rng) in (list, tuple):
        datums = rng
    else:
        raise TypeError('vbalinks.get_arrays: <rng> parameter should be an Excel range, or a list/tuple')
    
    parsed = []
    
    # Loop through each cell in the range
    for theRow, cellVal in enumerate(datums):
        if cellVal:
            cell = []
            
            # Loop through each list value in the cell (separated by ',')
            # Splits on ',' surrounded by any amount of whitespace (including no whitespace)
            for listVal in re.split(r'(?:\s*),(?:\s*)', cellVal):
                elem = []
                
                # Loop through each dimension in the list value (separated by ':')
                # Same as above, but splits on ':'
                for dimIndex, theDim in enumerate(re.split(r'(?:\s*):(?:\s*)', listVal)):
                    
                    if dimTypes[dimIndex] is datetime.datetime:
                        # Converting the text in the dimension into a datetime, then converting that back into a string
                        try:
                            theDate = str_to_datetime(theDim)
                        except:
                            raise ValueError("Can't convert '{}' to datetime. (Sheet: {}, Row: {})"
                                             .format(theDim, sh.name, theRow + 2))
                        else:
                            elem.extend([theDate.strftime('%m/%d/%Y')])
                        
                    else:  # dimTypes[dimIndex] in [int, str]:
                        elem.extend([theDim])
                        
                cell.append(elem)
            parsed.append(cell)
        else:
            parsed.append([])

    return parsed
                    
        
def get_arrays_text(sh, rng, dimTypes):  # sh is only needed for error messages
    """
    DEPRECATED
    Formats each cell of range <rng> into a format used for a postgres array type.
    The text in each Excel cell must be formatted with commas or semicolons separating elements. Colons add extra
    dimensions to each element.
    Parameters: sh, rng, dimTypes (a list/tuple of the Python data types of each dimension the array elements)
    Returns: a list of lists representing the data from the entire column. Each inner list contains 1 postgres array.
    """
    
    if type(rng) is xw.main.Range:
        datums = rng.value
    elif type(rng) in (list, tuple):
        datums = rng
    else:
        raise TypeError('vbalinks.get_arrays: <rng> parameter should be an Excel range, or a list/tuple')
    
    parsed = []
    
    # Loop through each cell in the range
    for theRow, cellVal in enumerate(datums):
        if cellVal:
            if cellVal.strip():  # In case there's a cell with nothing but whitespace, which we would just want to skip
                cell = []
                
                # Loop through each element in the cell (separated by ',' or ';')
                # Splits on ',' or ';' surrounded by any amount of whitespace (including no whitespace)
                for theElem in re.split(r'(?:\s*)(?:[,;])(?:\s*)', cellVal):
                    elem = []
                    
                    # Loop through each dimension in the element (separated by ':')
                    # Same as above, but splits on ':'
                    for dimIndex, theDim in enumerate(re.split(r'(?:\s*):(?:\s*)', theElem)):
                        
                        if dimTypes[dimIndex] is datetime.datetime:
                            # Converting the text in the dimension into a datetime, then converting back into a string
                            try:
                                theDate = str_to_datetime(theDim)
                            except:
                                raise ValueError("Can't convert '{}' to datetime. Sheet: {}, Row: {}."
                                                 .format(theDim, sh.name, theRow + 2))
                            else:
                                elem.extend([theDate.strftime('%x')])
                            
                        else:  # dimTypes[dimIndex] in [int, str]:
                            elem.extend([theDim])
                            
                    cell.append(elem)
                parsed.append(cell)
            else:
                parsed.append([])
        else:
            parsed.append([])

    return parsed
    

if __name__ == '__main__':
    xw.Book('hur.xlsx').set_mock_caller()
    sql_to_xl('SKUs', wkbk=xw.Book.caller(), sh='Sheet1',
              where='WHERE discontinue = TRUE AND enroute + stock + processing + available = 0')
#     test_get_arrays()
#     test_insert_lists()
