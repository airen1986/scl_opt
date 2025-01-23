from .populate_periods import main as populate_periods
from .validation_queries import primary_keys
from .populate_defaults import update_defaults
from ..sql_connector import insert_log

def round_decimals(conn, round_decimals = 6):
    ''' This method would round all numeric fields to 6 decimals from I_% tables'''
    get_table_sql = """select tbl_name from sqlite_schema
                        where type = 'table'
                        and substr(tbl_name, 1,2) = 'I_'"""
    table_names = conn.execute(get_table_sql).fetchall()

    for table_name in table_names:
        get_col_sql = "select name from pragma_table_info(?) WHERE type = 'NUMERIC'"
        col_names = conn.execute(get_col_sql, table_name).fetchall()
        for col_name in col_names:
            c_name = col_name[0]
            t_name = table_name[0]
            update_query = f"""UPDATE [{t_name}]
                                SET [{c_name}] = round([{c_name}], {round_decimals})
                                WHERE instr(CAST([{c_name}] as STRING), '.') > 0
                                and   length(CAST([{c_name}] as STRING)) - 
                                   instr(CAST([{c_name}] as STRING), '.') > {round_decimals}
                                and   round([{c_name}], {round_decimals}) != 0"""
            conn.execute(update_query)

update_stock_entry_date = """UPDATE I_OpeningStocks
                SET EntryDate = I_ModelSetup.StartDate
                FROM I_ModelSetup
                WHERE IFNULL(EntryDate, I_ModelSetup.StartDate) <= I_ModelSetup.StartDate"""

update_registration_dates = """UPDATE I_ForecastRegistration
                                SET StartDate = I_ModelSetup.StartDate
                                FROM I_ModelSetup
                                WHERE IFNULL(I_ForecastRegistration.StartDate, I_ModelSetup.StartDate) <= I_ModelSetup.StartDate ;
                                UPDATE I_ForecastRegistration
                                SET EndDate = t1.PeriodEndDate
                                FROM (
                                select Max(PeriodStart) as PeriodEndDate FROM O_Period
                                ) t1
                                WHERE IFNULL(I_ForecastRegistration.EndDate, t1.PeriodEndDate) >= t1.PeriodEndDate;"""

bom_select_query = """select distinct dob.ItemId, dob.LocationId
                        from TEMP.ActiveCombinations ac,
                            I_InventoryPolicy di,
                            I_Processes dop,
                            I_BOMRecipe dob
                        LEFT JOIN 
                            TEMP.ActiveCombinations ac2 ON dob.ItemId = ac2.ItemId AND     
                            dob.LocationId = ac2.LocationId
                        WHERE ac.ItemId = di.ItemId
                        AND   ac.LocationId = di.LocationId
                        and   di.IsProduction = 1
                        AND   di.ItemId = dop.ItemId
                        and   di.LocationId = dop.LocationId
                        and   dop.BOMId = dob.BOMId
                        and   dop.LocationId = dob.LocationId
                        AND   dob.UsageQuantity > 0
                        AND   ac2.ItemId is null"""

def main(conn):
    insert_log(conn, f"{'-'* 5} Model Clean Started {'-'* 5}")
    delete_null_primary_keys(conn)
    create_combinations(conn)
    delete_inactives(conn)
    conn.execute(update_stock_entry_date)
    conn.execute(update_registration_dates)
    round_decimals(conn)
    update_defaults(conn)
    insert_log(conn, f"{'-'* 5} Model Clean Completed {'-'* 5}")

def create_combinations(conn):
    '''This method creates all item location combinations (which have any dependency on demand)
       in TEMP.ActiveCombinations table, so that any other item/location can be deleted later in 
       delete_inactives method'''

    populate_periods(conn) #populate periods in O_Period table based on model horizon
    delete_zero_forecast =   """DELETE from I_ForecastOrders
                                WHERE Quantity <= 0"""
    conn.execute(delete_zero_forecast)
    delete_forecast = """DELETE from I_ForecastOrders
                         WHERE ForecastArrivalDate not in (SELECT PeriodStart From O_Period)"""
    conn.execute(delete_forecast)

    conn.execute("DROP TABLE IF EXISTS TEMP.ActiveCombinations")
    #insert item location combinations based on forecast and registration calendar (item_id, location_id)
    query = """ CREATE TABLE TEMP.ActiveCombinations
                AS
                SELECT DISTINCT ifnull(I_ForecastRegistration.ItemId, I_ForecastOrders.ItemId) AS ItemId,
                                I_ForecastOrders.LocationId
                FROM I_ForecastOrders
                    LEFT JOIN
            I_ForecastRegistration ON I_ForecastOrders.ItemId = I_ForecastRegistration.ForecastItemId AND 
                                        I_ForecastOrders.LocationId = I_ForecastRegistration.LocationId AND 
                                        I_ForecastOrders.ForecastArrivalDate >= I_ForecastRegistration.StartDate AND 
                                        I_ForecastOrders.ForecastArrivalDate <= I_ForecastRegistration.EndDate
                WHERE I_ForecastOrders.Quantity > 0"""
    conn.execute(query)

    # insert combination from forecast which are not in active combinations
    query = """INSERT INTO TEMP.ActiveCombinations (ItemId, LocationId)
                select distinct df.ItemId, df.LocationId
                from I_ForecastOrders df
                LEFT JOIN TEMP.ActiveCombinations ac
                ON df.ItemId = ac.ItemId
                and df.LocationId = ac.LocationId
                WHERE df.Quantity > 0
                and   ac.ItemId is null"""
    conn.execute(query)

    #insert upstream item, location in TEMP.ActiveCombinations table based on upstream transportation
    propogate_distribution(conn)
    ct = 1
    while ct > 0:
        row = conn.execute(bom_select_query).fetchone()
        if row:
            #insert upstream item, location in TEMP.ActiveCombinations table based on bills of materials
            propogate_BOM(conn)
            #insert upstream item, location in TEMP.ActiveCombinations table based on transportation policy
            propogate_distribution(conn)
        else:
            ct = 0


def propogate_distribution(conn):
    '''This method inserts item location combination in TEMP.ActiveCombinations table based on upstream 
        transportation policy of existing combinations in TEMP.ActiveCombinations table'''
    ct = 1
    while ct > 0:
        conn.intermediate_commit()
        #insert ItemId, FromLocation based on transportation policy
        add_query = """ INSERT INTO TEMP.ActiveCombinations (ItemId, LocationId)
                        SELECT DISTINCT dt.ItemId,
                                        dt.FromLocationId
                        FROM TEMP.ActiveCombinations ac,
                            I_TransportationPolicy dt
                            LEFT JOIN
                            TEMP.ActiveCombinations ac2 ON dt.ItemId = ac2.ItemId AND 
                                                        dt.FromLocationId = ac2.LocationId
                        WHERE ac.ItemId = dt.ItemId AND 
                            ac.LocationId = dt.ToLocationId AND 
                            ac2.ItemId IS NULL"""
        conn.execute(add_query)
        ct = conn.execute("select changes()").fetchone()[0]

def propogate_BOM(conn):
    '''This method inserts item location combination in TEMP.ActiveCombinations table based on upstream 
        bill of materials of existing combinations in TEMP.ActiveCombinations table'''
    ct = 1
    while ct > 0:
        conn.intermediate_commit()
        #insert InputItemId, Location based on bills of materials
        add_query = f"""INSERT INTO TEMP.ActiveCombinations (ItemId, LocationId)
                         {bom_select_query} """

        conn.execute(add_query)
        ct = conn.execute("select changes()").fetchone()[0]

def delete_inactives(conn):
    '''This method deletes all item/locations which doesn't exist in TEMP.ActiveCombinations table
        It then deletes all inactive resource and bill of materials
        It then deletes all records where period lies out of model horizon'''
    table_names = ["I_BOMRecipe", "I_InventoryPolicy", "I_ForecastOrders", 
                    "I_InventoryPolicyPerPeriod", "I_Processes", "I_ProcessesPerPeriod",
                    "I_ForecastRegistration", "I_OpeningStocks" ]

    # We will now delete all other combinations from different tables except records in TEMP.ActiveCombinations table,
    for table_name in table_names:
        delete_query = f"""DELETE FROM {table_name}
                        WHERE rowid in 
                        (
                        select dbi.rowid
                        from {table_name} dbi
                        LEFT JOIN TEMP.ActiveCombinations sc
                        ON dbi.ItemId = sc.ItemId
                        and dbi.LocationId = sc.LocationId
                        WHERE sc.ItemId is null
                        )"""
        conn.execute(delete_query)

    # Delete items except records in TEMP.ActiveCombinations table
    delete_item = """DELETE FROM I_ItemMaster
                    WHERE ItemId NOT IN (
                    SELECT DISTINCT ItemId
                    FROM TEMP.ActiveCombinations
                            )"""    
    conn.execute(delete_item)


    delete_locations = """DELETE FROM I_LocationMaster
                            WHERE LocationId NOT IN (
                            SELECT DISTINCT LocationId
                            FROM TEMP.ActiveCombinations
                        )"""
    conn.execute(delete_locations)

    delete_registration_calendar = """DELETE FROM I_ForecastRegistration
                                        WHERE rowid in 
                                        (
                                        select dbi.rowid
                                        from I_ForecastRegistration dbi
                                        LEFT JOIN TEMP.ActiveCombinations sc
                                        ON dbi.ForecastItemId = sc.ItemId
                                        and dbi.LocationId = sc.LocationId
                                        WHERE sc.ItemId is null  )"""
    conn.execute(delete_registration_calendar)

    delete_registration_calendar = """DELETE FROM I_ForecastRegistration
                                        WHERE rowid in
                                        (
                                            SELECT dr.rowid
                                            FROM I_ForecastRegistration dr,
                                                    ( select min(periodStart) as min_period, max(periodStart) as max_period
                                                        FROM O_Period
                                                    ) t2
                                            WHERE dr.StartDate > t2.max_period
                                            OR    dr.EndDate < t2.min_period
                                        )"""
    conn.execute(delete_registration_calendar)

    delete_resource = """DELETE FROM I_ResourceMaster
                            WHERE rowid IN (
                            SELECT dbi.rowid
                            FROM I_ResourceMaster dbi
                                LEFT JOIN
                                I_Processes sc ON dbi.ResourceId = sc.ResourceId AND 
                                                            dbi.LocationId = sc.LocationId
                            WHERE sc.ItemId IS NULL
                        )"""
    conn.execute(delete_resource)

    delete_bom = """DELETE FROM I_BOMRecipe
                            WHERE rowid IN (
                            SELECT dbi.rowid
                            FROM I_BOMRecipe dbi
                                LEFT JOIN
                                I_Processes sc ON dbi.BOMId = sc.BOMId AND 
                                                            dbi.LocationId = sc.LocationId
                            WHERE sc.ItemId IS NULL
                        )"""
    conn.execute(delete_bom)

    delete_redundant_bom = """DELETE FROM I_BOMRecipe
                              WHERE UsageQuantity = 0"""
    conn.execute(delete_redundant_bom)

    delete_resource_period = """DELETE from I_ResourcePerPeriod
                                    WHERE ResourceId NOT IN
                                        (SELECT DISTINCT ResourceId FROM I_ResourceMaster)"""
    conn.execute(delete_resource_period)

    dt_dict = {'I_TransportationPolicy': ['FromLocationId', 'ToLocationId'],
                'I_TransportationPolicyPerPeriod': ['FromLocationId', 'ToLocationId']}
    
    for table_name in dt_dict:
        for location_column in dt_dict[table_name]:
            delete_query = f"""DELETE FROM {table_name}
                        WHERE rowid in 
                        (
                        select dbi.rowid
                        from {table_name} dbi
                        LEFT JOIN TEMP.ActiveCombinations sc
                        ON dbi.ItemId = sc.ItemId
                        and dbi.{location_column} = sc.LocationId
                        WHERE sc.ItemId is null
                        )"""
            conn.execute(delete_query)

    periodic_tables = ['I_ProcessesPerPeriod', 'I_ResourcePerPeriod','I_TransportationPolicyPerPeriod']

    for table_name in periodic_tables:
        delete_query = f"""DELETE from {table_name}
                           WHERE StartDate not in ( select PeriodStart from O_Period)"""
        conn.execute(delete_query)
    
    update_null_bom = """UPDATE I_Processes
                            SET BOMId = null
                            WHERE rowid in
                            (
                            SELECT rowid
                            FROM I_Processes 
                            WHERE BOMId is not null
                            AND   BOMId not in (SELECT BOMId FROM I_BOMRecipe)
                            )"""
    conn.execute(update_null_bom)



def delete_null_primary_keys(conn):
    for table_name in primary_keys:
        for col_name in primary_keys[table_name]:
            query = f"""DELETE FROM [{table_name}]
                        WHERE [{col_name}] is null"""
            conn.execute(query)


