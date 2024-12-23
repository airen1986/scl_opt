from .validation_queries import *
from ..sql_connector import insert_log
from .populate_periods import main as populate_periods

def main(conn):
    insert_log(conn, f"{'-'* 5} Model Validation Started {'-'* 5}")
    conn.execute("DELETE FROM O_ModelValidation")
    validate_primary_keys(conn)
    validate_periods(conn)
    populate_periods(conn)
    validate_foreign_keys(conn)
    check_min_max_values(conn)
    verify_numeric_values(conn)
    run_validation_queries(conn)
    detect_transport_loop(conn)
    detect_bom_loop(conn)
    string_length_validation(conn)
    max_decimal_validation(conn)
    insert_log(conn, f"{'-'* 5} Model Validation Completed {'-'* 5}")

def validate_primary_keys(conn):
    for table_name in primary_keys:
        col_names = list(primary_keys[table_name])
        select_query = f"""SELECT   '{table_name}', 
                                    '({','.join(col_names)})',
                                    '('||[{"]||','||[".join(col_names)}]||')',
                                    'Error' ,
                                    'Multiple primary keys, count: '||count(*)
                            FROM [{table_name}]
                            GROUP BY [{'],['.join(col_names)}]
                            HAVING COUNT(*) > 1"""
        conn.execute(insert_query + select_query)
        for col_name in col_names:
            select_query = f"""SELECT  '{table_name}',
                                      '{col_name}',
                                    [{col_name}],
                                    'Error',
                                    'NULL PK, Check if there are any empty rows for this column'
                                FROM [{table_name}]
                                WHERE [{col_name}] IS NULL
                                LIMIT 1"""
            conn.execute(insert_query + select_query)
    insert_log(conn, "Primary key verification completed")


def validate_periods(conn):
    ''' All validations w.r.t I_ModelSetup table
        1. I_ModelSetup should have just 1 record,
        2. Valid start date,
        3. Valid number of periods
        4. valid time frequency'''
    query = f"SELECT date(StartDate), TimeFrequency, NumberOfPeriods FROM I_ModelSetup"
    all_rows = conn.execute(query).fetchall()
    query_tpl = None
    if len(all_rows) > 1:
        query_tpl = ('I_ModelSetup', None, None, "Error", f"Multiple rows are not allowed in I_ModelSetup table")    
    elif len(all_rows) == 0:
        query_tpl = ('I_ModelSetup', None, None, "Error", f"No data in I_ModelSetup table")

    if query_tpl:
        conn.execute(insert_query, query_tpl)
        return

    start_date, time_bucket, period_count = all_rows[0]

    if start_date is None:
        query_tpl = ('I_ModelSetup', 'StartDate', start_date, "Error", "Not a valid date")

    elif period_count is None:
        query_tpl = ('I_ModelSetup', 'NumberOfPeriods', period_count, "Error", "Please define at least 1 period")
    elif not float(period_count).is_integer():
        query_tpl = ('I_ModelSetup', 'NumberOfPeriods', period_count, "Error", "Number of periods should be integer")
    elif period_count <= 0 or period_count >= 1000:
        query_tpl = ('I_ModelSetup', 'NumberOfPeriods', period_count, "Error", "Invalid number of periods, period count < 1000")
    elif time_bucket not in ('Daily', 'Weekly', 'Monthly', 'Quarterly', 'Yearly'):
        query_tpl = ('I_ModelSetup', 'TimeFrequency', time_bucket, "Error", "Invalid time Bucket")
    
    if query_tpl:
        conn.execute(insert_query, query_tpl)
        return

    validation_query = """ select 'I_ModelSetup', 'StartDate', StartDate, 'Error', 'Start of Period should start on Monday' 
                            from I_ModelSetup
                            where TimeFrequency = 'Weekly'
                            and   strftime('%w', date(StartDate)) != '1'
                            UNION
                            select 'I_ModelSetup', 'StartDate', date(StartDate), 'Date Error', 'Start of Period should start on first day of month' 
                            from I_ModelSetup
                            where TimeFrequency = 'Monthly'
                            and   substr(date(StartDate),9,2) != '01'
                            UNION
                            select 'I_ModelSetup', 'StartDate', date(StartDate), 'Error', 'Start of Period should start on first day of quarter' 
                            from I_ModelSetup
                            where TimeFrequency = 'Quarterly'
                            and substr(date(StartDate),6,5) not in ('01-01', '04-01', '07-01', '10-01')
                            UNION
                            select 'I_ModelSetup', 'StartDate', date(StartDate), 'Error', 'Start of Period should start on first day of year' 
                            from I_ModelSetup
                            where TimeFrequency = 'Yearly'
                            and substr(date(StartDate),6,5) not in ('01-01')"""                    
    conn.execute(insert_query + validation_query)
    insert_log(conn, "Period verification completed")

def validate_foreign_keys(conn):
    ''' 1. Check if corresponding record exist in master table, for e.g.
            a. Item in I_InventoryPolicy should exists in I_ItemMaster table
            b. Item, Location in I_Processes table should exists in I_InventoryPolicy table
        All such relation exists in "foreign_keys" data structure '''
    
    for fk_dict, prim_dict in foreign_keys:
        fk_table = list(fk_dict.keys())[0]
        prim_table = list(prim_dict.keys())[0]
        col_names = fk_dict[fk_table]
        select_query = f"""SELECT DISTINCT  '{fk_table}', 
                                    '({','.join(col_names)})',
                                    '('||t1.[{"]||','||t1.[".join(col_names)}]||')',
                                    'Error' ,
                                    'FK: Row: ('||t1.[{"]||','||t1.[".join(col_names)}]||') not in {prim_table}'
                            from [{fk_table}] t1
                            LEFT JOIN [{prim_table}] t2
                            ON 1 = 1 """
        for i in range(len(fk_dict[fk_table])):
            select_query += f" AND t1.[{fk_dict[fk_table][i]}] = t2.[{prim_dict[prim_table][i]}]"
        select_query += f""" WHERE t1.[{fk_dict[fk_table][i]}] is not null
                            AND  t2.[{prim_dict[prim_table][i]}] is null"""
        conn.execute(insert_query + select_query)
    insert_log(conn, "Foreign key verification completed")

def check_min_max_values(conn):
    ''' Based on min_max_constraints  data structure, it validates that all max values should not be 
        less than min values'''
    for table_name, min_column, max_column in min_max_constraints:
        table_columns = list()
        table_columns.append(max_column)
        query = f""" select  DISTINCT 
                                '{table_name}', 
                                '{','.join(table_columns)}', 
                                 [{"]||', '||[".join(table_columns)}], 
                                'Error', 
                                'Value should be numeric else INF'
                        from {table_name}
                        WHERE CASE
                                WHEN {max_column} is NULL THEN 1
                                WHEN {max_column} = 'INF' THEN 1
                                WHEN {max_column} = CAST({max_column} as INTEGER) THEN 1
                                WHEN {max_column} = CAST({max_column} as Real) THEN 1
                                ELSE 0 END = 0"""
        conn.execute(insert_query + query)

        table_columns.append(min_column)
        query = f"""    select  DISTINCT
                                '{table_name}', 
                                '{','.join(table_columns)}', 
                                 [{"]||', '||[".join(table_columns)}], 
                                'Error', 
                                'Max Value should be greater than Min Value'
                        from {table_name}
                        where ifnull({max_column}, 0) != 'INF'
                        AND   CAST(ifnull({max_column},999999999) as Real) < ifnull({min_column},0)"""
        conn.execute(insert_query + query)
    insert_log(conn, "Min Max check completed")


def verify_numeric_values(conn):
    ''' 1. Based on positive_vals data structure, it validates if there are any 0, null or negative values
            for such columns
        2. Based on non_negative_vals data structure, check if there are any negatives for such columns
        3. Based on max_1_values data structure, check if there are any > 1 values for such columns
        4. Based on boolean_values data structure, check if there are non booleans for such columns'''
    for table_name in positive_vals:
        for col_name in positive_vals[table_name]:
            table_columns = list()
            table_columns.append(col_name)
            select_query = f"""select   DISTINCT
                                        '{table_name}', 
                                        '{','.join(table_columns)}', 
                                        [{"]||', '||[".join(table_columns)}], 
                                        'Error', 
                                        '{col_name} should not be 0'
                                from [{table_name}]
                                WHERE CAST(ifnull([{col_name}],0) AS Real) = 0
                                UNION
                                select   DISTINCT
                                        '{table_name}', 
                                        '{','.join(table_columns)}', 
                                         [{"]||', '||[".join(table_columns)}], 
                                        'Error', 
                                        'Cannot be negative value'
                                from [{table_name}]
                                WHERE CAST(ifnull([{col_name}],0) AS Real) < 0
                                UNION
                                select   DISTINCT
                                        '{table_name}', 
                                        '{','.join(table_columns)}', 
                                         [{"]||', '||[".join(table_columns)}], 
                                        'Warning', 
                                        'Please check your model, such high values are not advised'
                                from [{table_name}]
                                WHERE CAST(ifnull([{col_name}],0) AS Real) > 1000000"""
            conn.execute(insert_query + select_query)
    for table_name in null_or_positive:
        table_cols = list(primary_keys.get(table_name, []))
        table_cols = list()
        if 'ScenarioName' in table_cols:
            table_cols.remove("ScenarioName")
        for col_name in null_or_positive[table_name]:
            table_columns = list(table_cols)
            table_columns.append(col_name)
            select_query = f"""select   DISTINCT
                                        '{table_name}', 
                                        '{','.join(table_columns)}', 
                                        [{"]||', '||[".join(table_columns)}], 
                                        'Error', 
                                        '{col_name} should be non zero positive'
                                from [{table_name}]
                                WHERE CAST(ifnull([{col_name}],1) AS Real) <= 0"""       
            conn.execute(insert_query + select_query)  


    for table_name in non_negative_vals:
        table_cols = list(primary_keys.get(table_name, []))
        table_cols = list()
        if 'ScenarioName' in table_cols:
            table_cols.remove("ScenarioName")
        for col_name in non_negative_vals[table_name]:
            table_columns = list(table_cols)
            table_columns.append(col_name)
            select_query = f"""select   DISTINCT
                                        '{table_name}', 
                                        '{','.join(table_columns)}', 
                                         [{"]||', '||[".join(table_columns)}], 
                                        'Negative Value Error', 
                                        'Cannot be  negative value'
                                from [{table_name}]
                                WHERE CAST(ifnull([{col_name}],0) AS Real) < 0"""
            conn.execute(insert_query + select_query)

            select_query = f"""select   DISTINCT
                                        '{table_name}', 
                                        '{','.join(table_columns)}', 
                                         [{"]||', '||[".join(table_columns)}],  
                                        'Warning: Numerical Instability', 
                                        'Please check your model, such high values are not advised'
                                from [{table_name}]
                                WHERE CAST(ifnull([{col_name}],0) AS Real) > 1000000"""
            conn.execute(insert_query + select_query)
    
    for table_name in max_1_values:
        table_cols = list(primary_keys.get(table_name, []))
        table_cols = list()
        if 'ScenarioName' in table_cols:
            table_cols.remove("ScenarioName")
        for col_name in max_1_values[table_name]:
            table_columns = list(table_cols)
            table_columns.append(col_name)
            select_query = f"""select   DISTINCT
                                        '{table_name}', 
                                        '{','.join(table_columns)}', 
                                         [{"]||', '||[".join(table_columns)}], 
                                        'Error', 
                                        'Cannot exceed 1'
                                from [{table_name}]
                                WHERE CAST(ifnull([{col_name}],0) AS Real) > 1"""
            conn.execute(insert_query + select_query)

    for table_name in boolean_values:
        table_cols = list(primary_keys.get(table_name, []))
        table_cols = list()
        if 'ScenarioName' in table_cols:
            table_cols.remove("ScenarioName")
        for col_name in boolean_values[table_name]:
            table_columns = list(table_cols)
            table_columns.append(col_name)
            select_query = f"""select   DISTINCT
                                        '{table_name}', 
                                        '{','.join(table_columns)}', 
                                         [{"]||', '||[".join(table_columns)}], 
                                        'Error', 
                                        'Please update 0/1 status'
                                from [{table_name}]
                                WHERE [{col_name}] not in (0,1)"""
            conn.execute(insert_query + select_query)
    insert_log(conn, "Numeric check completed")


def run_validation_queries(conn):
    ''' 1. Check if there is any broken link, or if there is no source for non manufacturing item 
        2. Check if no operation process for IsProduction inventory
        3. Check if there are mutiple item/locations per process id
        4. Check if there are both inbound and make policies, or if there is any inventory policy which doesnt have any source or destination
        5. Check if DOSWindow is multiple of period frequency days or non zero DOSWindow in case SafetySockDOS > 0
        6. Check if SupplyCapacity is null in I_ResourceMaster table
        7. Check if a record in I_Processes has IsProduction=0 in I_InventoryPolicy table
        8. Give warning if there are any inactive items in I_ItemMaster table
        9. Give warning if sales price is less tha unit cost in I_ItemMaster table
        '''
    conn.execute(insert_query + no_source_query) #1
    conn.execute(insert_query + no_process_code) #2
    conn.execute(insert_query + duplicate_processes) #3
    conn.execute(insert_query + sourcing_warning) #4
    conn.execute(insert_query + dos_window_error) #5
    conn.execute(insert_query + null_supply_capacity_query) #6
    conn.execute(insert_query + no_manufacturing_warning) #7
    conn.execute(insert_query + inactive_items) #8
    conn.execute(insert_query + unit_cost_sales_price_check) #9

    insert_log(conn, "Validation queries executed")

def detect_transport_loop(conn):
    '''Check if there are any loops in I_TransportationPolicy table '''
    select_query = "select DISTINCT t0.ItemId, t0.FromLocationId, t0.ToLocationId "
    from_query = " from I_TransportationPolicy t0 "
    where_query = " WHERE 1 = 1 "
    i = 1
    ct = 1
    while ct > 0:
        select_query += f" , t{i}.ToLocationId"
        from_query += f" ,I_TransportationPolicy t{i} "
        where_query += f" AND  t{i-1}.ItemId = t{i}.ItemId and  t{i-1}.toLocationId = t{i}.fromLocationId"
        full_query = select_query + from_query + where_query
        row = conn.execute(full_query).fetchone()
        if row is None:
            ct = 0
        else:
            full_query += f" AND t0.FromLocationId = t{i}.ToLocationId"
            rows = conn.execute(full_query).fetchall()
            if len(rows) > 0:
                for row in rows:
                    insert_tpl = ('I_TransportationPolicy', 'ItemId, FromLocationId, ToLocationId, ..',
                                    str(row), 'Error', 'Please check circular relations in TransportationPolicy')
                    conn.execute(insert_query, insert_tpl)
                ct = 0
        i += 1
    insert_log(conn, "Transportation loop check completed")        


def detect_bom_loop(conn):
    '''Check if there are any loops in I_Processes, I_BOMRecipe table '''
    select_query = """WITH temp_bom
                        AS
                        (
                            select DISTINCT I_BOMRecipe.ItemId as from_item, I_Processes.ItemId as to_item
                            from I_Processes,
                                I_BOMRecipe 
                            WHERE I_Processes.BOMId = I_BOMRecipe.BOMId
                        )
                        SELECT t0.from_item, t0.to_item as to_item_0 """
    from_query = " FROM temp_bom t0 "
    where_query = " WHERE 1 = 1 "
    i = 1
    ct = 1
    while ct > 0:
        select_query += f" , t{i}.to_item as to_item_{i}"
        from_query += f" ,temp_bom t{i} "
        where_query += f" AND  t{i-1}.to_item = t{i}.from_item"
        full_query = select_query + from_query + where_query
        row = conn.execute(full_query).fetchone()
        if row is None:
            ct = 0
        else:
            full_query += f" AND t0.from_item = t{i}.to_item"
            rows = conn.execute(full_query).fetchall()
            if len(rows) > 0:
                for row in rows:
                    insert_tpl = ('I_BOMRecipe', 'FromItemId, ToItemId, ..',
                                    str(row), 'Error', 'Please check circular relations in BOMRecipe')
                    conn.execute(insert_query, insert_tpl)
                ct = 0
        i += 1        
    insert_log(conn, "BOM loop check completed")


def string_length_validation(conn):
    ''' 1. Validate if there are any primary column values of length greater than 255 characters '''

    for table_name in primary_keys:
        column_names = list(primary_keys[table_name])

        for column_name in column_names:
            select_query = f""" SELECT DISTINCT '{table_name}', 
                                '{column_name}', 
                                [{column_name}],
                                'Warning', 
                                'Column value exceeding maximum length of 255 chars'
                                FROM [{table_name}]
                                WHERE length([{column_name}]) > 255"""
            conn.execute(insert_query + select_query)
    insert_log(conn, "String length validated")


def max_decimal_validation(conn, round_decimals = 5):
    ''' Validate if all numeric fields have less than 5 decimals '''
    get_table_sql = """select tbl_name from sqlite_schema
                        where type = 'table'
                        and substr(tbl_name, 1,2) = 'I_'"""
    table_names = conn.execute(get_table_sql).fetchall()

    for table_name in table_names:
        get_col_sql = "select name from pragma_table_info(?) WHERE type = 'NUMERIC'"
        col_names = conn.execute(get_col_sql, table_name).fetchall()
        table_cols = list(primary_keys.get(table_name[0], []))
        
        for col_name in col_names:
            table_columns = list(table_cols)
            table_columns.append(col_name[0])

            c_name = col_name[0]
            t_name = table_name[0]
            select_query = f""" SELECT DISTINCT '{t_name}', 
                                '{','.join(table_columns)}', 
                                [{"]||', '||[".join(table_columns)}],
                                'Warning', 
                                'Column value exceeding max {round_decimals} decimals'
                                FROM [{t_name}]
                                WHERE instr(CAST([{c_name}] as STRING), '.') > 0
                                and   length(CAST([{c_name}] as STRING)) - 
                                instr(CAST([{c_name}] as STRING), '.') > {round_decimals}"""
            conn.execute(insert_query + select_query)

    insert_log(conn, "Decimals validated")