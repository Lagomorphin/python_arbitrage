import xlwings as xw

from AmazonSelling.inventory import update_skus
from AmazonSelling.tools import call_sql, con_postgres
from ExcelIntegration.xlfuncs import sql_to_xl, xl_to_sql


######################################
### Department Totals Updater.xlsm ###
######################################
def dept_totals_updater_tosql():
    """
    To be called from VBA
    """
    
    xl_to_sql('public.Az_Depts', xw.Book.caller(), sh='Results', upsert=True)


#########################
### Display Data.xlsm ###
#########################
def displaydata_toxl_manual():
    """
    Writes the postgres view public.Display1 to "Display Data.xlsm"
    """
     
    sql_to_xl('Display1', wkbk=xw.Book.caller(), sh='Manual')

      
def displaydata_tosql_manual():
    
    xl_to_sql('io.Manual', xw.Book.caller(), sh='Manual', upsert=True)
    
    
def displaydata_toxl_already():
    
    con = con_postgres()
    call_sql(con, 'REFRESH MATERIALIZED VIEW public."Already"', [], 'executeNoReturn')
    con.close()
    
    sql_to_xl('Already', wkbk=xw.Book.caller(), sh='Already')
    
    
def displaydata_tosql_already():
    
    alreadySh = xw.Book.caller().sheets('Already')
    
    fullRange = alreadySh.range('A1').current_region
    _headersRow = fullRange.rows(1)
    datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    
    # Build theData from the colsNames columns in alreadySh
    # The order matters, needs to match the SQL statement
    colsNames = ('fragile', 'polybag', 'expire', 'ingredients', 'discontinue', 'edit', 'non_joliet', 'sku')
    theData = []
    for i in range(1, datumsRange.rows.count + 1):
        theData.append([datumsRange.columns(_headersRow.value.index(_g) + 1)(i).value for _g in colsNames])

    con = con_postgres()
    
    sqlTxt = '''UPDATE io."SKUs"
                SET fragile = %s, polybag = %s, expire = %s, ingredients = %s, discontinue = %s, edit = %s, non_joliet = %s
                WHERE sku = %s'''
    call_sql(con, sqlTxt, theData, 'executeBatch')
    
    if con:
        con.close()
        
    
def displaydata_tosql_purchased():
    """
    Adds new rows to io.Purchased for bought items.
    """

    xl_to_sql("io.Purchased", xw.Book.caller(), sh='Purchased', upsert=False)
     
    update_skus()
    
        
def displaydata_checkskus():
    """
    Checks each asin in purchasedSh to see if a sku already exists for it in io.SKUs. If so, overwrite the sku in
    purchasedSh with the old sku from SQL.
    """
    
    purchasedSh = xw.Book.caller().sheets('Purchased')
    
    fullRange = purchasedSh.range('A1').current_region
    headersRow = fullRange.rows(1)
    datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    skusCol = datumsRange.columns(headersRow.value.index('sku') + 1)
        
    asins = [datumsRange.columns(headersRow.value.index('asin') + 1)(i).value
             for i in range(1, datumsRange.rows.count + 1)]
    
    con = con_postgres()
    
    sqlTxt = '''SELECT sku
                FROM "SKUs"
                WHERE asin = %s'''
    
    for i in range(0, len(asins)):              
        skus = tuple(j[0] for j in call_sql(con, sqlTxt, [asins[i]], 'executeReturn'))            
        if len(skus) > 0:
            # Use the shortest matching SKU, i.e. the one that doesn't have a used identifier (ULN, UVG, etc.)
            skusCol(i + 1).formula = min(skus, key=len)
        else:
            continue
            
    if con:
        con.close()

        
def displaydata_expandrows():
    """
    Scans the active sheet (will only actually activate on purchasedSh) for asins or wm_ids that were added manually,
    i.e. have no other data in their rows.
    Fills in the what empty columns it can from Products_WmAz, and additionally from io.SKUs to
    get thecolumn <my_name>.
    """
    
#     shtName = xw.Range('A1').sheet.name
    fullRange = xw.Range('A1').current_region
    headersRow = fullRange.rows(1)
    datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    asinCol =  datumsRange.columns(headersRow.value.index('asin') + 1)
    wmidCol =  datumsRange.columns(headersRow.value.index('wm_id') + 1)
    skuCol =  datumsRange.columns(headersRow.value.index('sku') + 1)
    
    con = con_postgres()
    
    sqlTxt = '''SELECT *
                FROM "{}"
                WHERE {} = %s'''
    
    for i in range(1, datumsRange.rows.count + 1):
        # This row has at most 2 filled-in columns (assumed to be asin or wm_id)
        if sum(xcv is not None for xcv in datumsRange.rows(i).value) <= 2:
            
            # If wm_id is filled in, but not asin, get the asin from Products_WmAz
            if wmidCol(i).value and not asinCol(i).value:
                asinCol(i).formula = call_sql(con, 'SELECT asin FROM "Products_WmAz" WHERE wm_id = %s',
                                              [wmidCol(i).value], 'executeReturn')[0]
            # If sku is filled in, but not asin, get the asin from io.SKUs
            elif skuCol(i).value and not asinCol(i).value:
                asinCol(i).formula = call_sql(con, 'SELECT asin FROM io."SKUs" WHERE sku = %s',
                                              [skuCol(i).value], 'executeReturn')[0]
                
            # Now, get the rest of the columns from Products_WmAz using the asin
            a, b = (None for _ in range(2))
            try:
                a = call_sql(con, sqlTxt.format('Products_WmAz', 'asin'), [asinCol(i).value], 'executeReturn',
                             dictCur=True)[0]
            except IndexError:  # No asins need to be expanded
                pass
            try:
                b = call_sql(con, sqlTxt.format('SKUs', 'sku'), [skuCol(i).value], 'executeReturn',
                             dictCur=True)[0]
            except IndexError:  # No asins need to be expanded
                pass
            
            # Put the values from SQL into purchasedSh
            headerVals = headersRow.value
            for j, header in enumerate(headerVals):
                if a:
                    if header in a.keys():
                        if a[header]:
                            datumsRange(i, j + 1).formula = str(a[header])
                    elif header == 'name':  # If io.SKUs has a value for my_name, use that. Else use az_name (again)
                        if b:
                            if b['my_name']:
                                datumsRange(i, j + 1).formula = str(b['my_name'])
                            elif a['az_name']:
                                datumsRange(i, j + 1).formula = str(a['az_name'])
                        elif a['az_name']:
                            datumsRange(i, j + 1).formula = str(a['az_name'])
                            
    if con:
        con.close()

    
def displaydata_expandrows_old():
    """
    Scans the active sheet for asins or wm_ids that were added manually, i.e. have no other data in their rows.
    Fills in the what empty columns it can from Products_WmAz, and additionally from io.Manual if the active
    sheet is manualSh.
    """
    
#     shtName = xw.Range('A1').sheet.name
    fullRange = xw.Range('A1').current_region
    headersRow = fullRange.rows(1)
    datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    asinsCol =  datumsRange.columns(headersRow.value.index('asin') + 1)
    wmidCol =  datumsRange.columns(headersRow.value.index('wm_id') + 1)

    con = con_postgres()
    
    # Get column names from io.Manual
    sqlTxt = """SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'Manual'"""
    availCols1 = [y[0] for y in call_sql(con, sqlTxt, [], 'executeReturn')]  # List of column names in io.Manual
    
    cols1 = [y for y in availCols1 if y in headersRow.value]
    formatters1 = ", ".join(cols1)
    
    # Get column names from Products_WmAz
    sqlTxt = """SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'Products_WmAz'"""
    availCols2 = [y[0] for y in call_sql(con, sqlTxt, [], 'executeReturn')]  # List of column names in Products_WmAz
    
    cols2 = [y for y in availCols2 if y in headersRow.value and y not in cols1]
    formatters2 = ", ".join(cols2)
    
    sqlTxt0 = '''SELECT asin
                 FROM io."Purchased"
                 WHERE wm_id = %s'''
                 
    sqlTxt2 = '''SELECT {}
                 FROM io."Manual"
                 WHERE asin = %s'''.format(formatters1)
                 
    sqlTxt1 = '''SELECT {}
                 FROM "Products_WmAz"
                 WHERE asin = %s'''.format(formatters2)
    
    for i in range(1, datumsRange.rows.count + 1):

        # This row has at most 2 filled-in columns (assumed to be asin or wm_id)
        if sum(xcv is not None for xcv in datumsRange.rows(i).value) <= 2:
            
            # If wm_id is filled in, but not asin, get the asin from io.Purchased
            if wmidCol(i).value and not asinsCol(i).value:
                asinsCol(i).formula = call_sql(con, sqlTxt0, [wmidCol(i).value], 'executeReturn')[0]
                
            # Now, get the rest of the columns from Products_WmAz (and possibly io.Manual) using the asin
            try:
                # Fetch data for this asin from Products_WmAz
                aa = call_sql(con, sqlTxt1, [asinsCol(i).value], 'executeReturn', dictCur=True)
                a = aa[0]  # Fetch data for this asin from io.Manual
            except IndexError:  # No asins need to be expanded
                a = None
            try:
                b = call_sql(con, sqlTxt2, [asinsCol(i).value], 'executeReturn', dictCur=True)[0]
            except IndexError:  # No asins need to be expanded
                b = None
            
            # Put the values from SQL into purchasedSh
            for j in headersRow:
                header = j.value
                if a:
                    if header in a.keys():  # and shtName == 'Manual':
                        if a[header]:
                            datumsRange(i, j.column).formula = str(a[header])
                if b:
                    if header in b.keys():
                        if b[header]:
                            datumsRange(i, j.column).formula = str(b[header])
    
    if con:
        con.close()

    
def displaydata_fillinvloader():
    """
    DEPRECATED
    Removes entries from invLoaderSh that are neither first-timers nor problem children (hazmat, restricted, etc)
    The only entries remaining in invLoaderSh should be first-timers and problem children
    """
    
    invLoaderSh = xw.Book.caller().sheets('Inv Loader')
     
    fullRange = invLoaderSh.range('A1').current_region
    headersRow = fullRange.rows(1)
    datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    asinsCol = datumsRange.columns(headersRow.value.index('product-id') + 1)
    
    con = con_postgres()
    
    sqlTxt = '''SELECT asin
                FROM "SKUs"
                WHERE asin NOT IN (
                   SELECT asin
                   FROM "Purchased"
                   WHERE lower(notes) LIKE '%%hazmat%%'
                   OR lower(notes) LIKE '%%restricted%%'
                )'''
    asinsToAxe = [hjk[0] for hjk in call_sql(con, sqlTxt, [], 'executeReturn')]    
    
    if con:
        con.close()

    
def displaydata_toxl_enroute():
    
    con = con_postgres()
    call_sql(con, 'REFRESH MATERIALIZED VIEW io."Enroute"', [], 'executeNoReturn')
    con.close()
    
    sql_to_xl('io.Enroute', wkbk=xw.Book.caller(), sh='Enroute')

    
def displaydata_tosql_enroute():
    
    enrouteSh = xw.Book.caller().sheets('Enroute')
    
    fullRange = enrouteSh.range('A1').current_region
    _headersRow = fullRange.rows(1)
    datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    
    # Build datums from the colsNames columns in enrouteSh

    # The order matters, needs to match the SQL statement
    colsNames = ['received', 'cancelled', 'lost', 'notes', 'purchase_id']
    theData = []
    for i in range(1, datumsRange.rows.count + 1):
        theData.append([datumsRange.columns(_headersRow.value.index(_g) + 1)(i).value for _g in colsNames])

    con = con_postgres()
    
    sqlTxt = '''UPDATE io."Purchased"
                SET received = %s, cancelled = %s, lost = %s, notes = %s
                WHERE purchase_id = %s'''
    call_sql(con, sqlTxt, theData, "executeBatch")
    
    if con:
        con.close()

        
def displaydata_toxl_instock():
    
    con = con_postgres()
    call_sql(con, 'REFRESH MATERIALIZED VIEW io."In_Stock"', [], "executeNoReturn")
    con.close()

    sql_to_xl('io.In_Stock', wkbk=xw.Book.caller(), sh='In Stock')

    
def displaydata_tosql_instock():
    
    isStockSh = xw.Book.caller().sheets('In Stock')
    
    fullRange = isStockSh.range('A1').current_region
    _headersRow = fullRange.rows(1)
    datumsRange = fullRange.resize(fullRange.rows.count - 1).offset(1, 0)
    
    # Build theData from the colsNames columns in instockSh

    # The order matters, needs to match the SQL statement
    colsNames1 = ['labeled', 'packing', 'lost', 'pack_date', 'notes', 'purchase_id']
    theData1 = []
    colsNames2 = ['non_joliet', 'sku']  # The order matters, needs to match the SQL statement
    theData2 = []
    for i in range(1, datumsRange.rows.count + 1):
        theData1.append([datumsRange.columns(_headersRow.value.index(_g) + 1)(i).value for _g in colsNames1])
        theData2.append([datumsRange.columns(_headersRow.value.index(_g) + 1)(i).value for _g in colsNames2])
    
    # Go through and turn any blank strings ('') into None. Postgres can't handle blank strings in non-text columns.
    # Could bo done inside the list compehension with a conditional, but that would be ugly...but also faster.
    for i, theRow in enumerate(theData1):
        for j, theColumn in enumerate(theRow):
            if theColumn == '':
                theData1[i][j] = None

    con = con_postgres()
     
    sqlTxt = '''UPDATE io."Purchased"
                SET labeled = %s, packed = COALESCE(packed, 0) + COALESCE(%s, 0), lost = %s, pack_date = %s, notes = %s
                WHERE purchase_id = %s'''
    call_sql(con, sqlTxt, theData1, "executeBatch")
    
    sqlTxt = '''UPDATE io."SKUs"
                SET non_joliet = %s
                WHERE sku = %s'''
    call_sql(con, sqlTxt, theData2, 'executeBatch')
    
    if con:
        con.close()


########################
### taxo_editor.xlsm ###
########################
def taxoeditor_tosql():
    """
    Writes taxo_editor.xlsm to WmTaxo_Updated
    """
    
    xl_to_sql('wm.WmTaxo_Updated', xw.Book.caller(), sh='Taxo', upsert=True)

    
def taxoeditor_toxl():
    """
    Writes WmTaxo_Updated to taxo_editor.xlsm
    """
    
    sql_to_xl('wm.WmTaxo_Updated', wkbk=xw.Book.caller(), sh='Taxo')


##############################
### Restricted Brands.xlsm ###
##############################
def restrictedbrands_tosql():
    
    xl_to_sql('io.Restr_Brands', xw.Book.caller(), sh='Brands', upsert=True)
    
    con = con_postgres()
    
    sqlTxt = '''UPDATE io."Restr_Brands"
                SET checked_date = CURRENT_DATE
                WHERE checked_date is Null'''
    call_sql(con, sqlTxt, [], 'executeNoReturn')
    
    if con:
        con.close()


if __name__ == '__main__':
    
    # xw.Book('Restricted Brands.xlsm').set_mock_caller()
    # restrictedbrands_tosql()

    xw.Book('Display Data.xlsm').set_mock_caller()
#     xw.Book('Display Data Shared.xlsm').set_mock_caller()
     
#     displaydata_toxl_manual()
#     displaydata_tosql_manual()
#     displaydata_toxl_already()
#     displaydata_tosql_already()
#     displaydata_tosql_purchased()
#     displaydata_checkskus()
#     displaydata_expandrows()
#     displaydata_fillinvloader()
#     displaydata_toxl_skus()
#     displaydata_toxl_enroute()
#     displaydata_tosql_enroute()
    displaydata_toxl_instock()
#     displaydata_tosql_instock()
    
#     sql_to_xl('io.SKUs', wkbk=xw.Book.caller(), sh='blurp')
#     sql_to_xl_OLD('io.SKUs', wkbk=xw.Book.caller(), sh='blurp')
#     xl_to_sql('io.SKUs1', xw.Book.caller(), sh='Sheet2', upsert=True)
