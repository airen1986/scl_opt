from .validation_queries import *
from ..sql_connector import insert_log
from .populate_periods import main as populate_periods
from .backup_and_restore import backup_tables, restore_tables
from .populate_defaults import update_defaults

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
    backup_tables(conn)
    update_defaults(conn)
    cost_validation(conn)
    split_ratio_validation(conn)
    restore_tables(conn)
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

    for table_name, min_column, max_column in min_max_dates:
        table_columns = list()
        table_columns.append(max_column)
        table_columns.append(min_column)

        query = f"""    select  DISTINCT
                                '{table_name}', 
                                '{','.join(table_columns)}', 
                                 [{"]||', '||[".join(table_columns)}], 
                                'Error', 
                                'Max Value should be greater than Min Value'
                        from {table_name}
                        where 1 = 1
                        AND   ifnull({max_column},'9999-12-31') < ifnull({min_column}, '1900-01-01')"""
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
        for col_name in null_or_positive[table_name]:
            table_columns = list()
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
        for col_name in non_negative_vals[table_name]:
            table_columns = list()
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
        for col_name in max_1_values[table_name]:
            table_columns = list()
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
        for col_name in boolean_values[table_name]:
            table_columns = list()
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


def cost_validation(conn):
    ''' Check if downstream cost is not lower than upstream cost, check are done on:
        1. Transportation policy check
        2. Bill of Materials check
        '''
    select_query = """SELECT DISTINCT 'I_InventoryPolicy',
                '(InputProduct, OutputProduct, InputCost, OutputCost)',
                '('||I_BOMRecipe.ItemId||','||I_Processes.ItemId||','||round(rmi.InventoryUnitCost * I_BOMRecipe.UsageQuantity,2)||','||round(fgi.InventoryUnitCost,2)||')',
                'Warning',
                'Upstream cost is lower than downstream in BOM'
                FROM I_Processes ,
                     I_InventoryPolicy rmi,
                     I_InventoryPolicy fgi,
                     I_BOMRecipe
                WHERE I_Processes.BOMId = I_BOMRecipe.BOMId
                AND   I_Processes.LocationId = I_BOMRecipe.LocationId
                AND   rmi.ItemId = I_BOMRecipe.ItemId
                AND   rmi.LocationId = I_BOMRecipe.LocationId
                AND   fgi.ItemId = I_Processes.ItemId
                AND   fgi.LocationId = I_Processes.LocationId
                AND   ifnull(fgi.InventoryUnitCost,0) < ifnull(rmi.InventoryUnitCost,0) * I_BOMRecipe.UsageQuantity"""
    conn.intermediate_commit()
    conn.execute(insert_query + select_query)

    select_query = """SELECT DISTINCT 'I_InventoryPolicy',
                                '(OutputProduct, InputCost, OutputCost)',
                                '('||output_item||','||round(min_cost,2)||','||round(di.InventoryUnitCost,2)||')',
                                'Warning',
                                'Upstream cost is lower than downstream in BOM aggregated'
                FROM
                (
                SELECT output_item, LocationId, sum(ifnull(InventoryUnitCost,0) * UsageQuantity) as min_cost
                FROM
                (
                    select DISTINCT db.ItemId as input_item, db.LocationId,  dop.ItemId as output_item, db.UsageQuantity, di.InventoryUnitCost
                    from I_BOMRecipe db,
                        I_Processes dop,
                        I_InventoryPolicy di
                    WHERE db.BOMId = dop.BOMId
                    AND   db.LocationId = dop.LocationId
                    AND   db.ItemId = di.ItemId
                    and   db.LocationId = di.LocationId
                )
                GROUP BY output_item, LocationId
                ) t1,
                I_InventoryPolicy di
                WHERE t1.output_item = di.ItemId
                AND   t1.LocationId = t1.LocationId
                and   t1.min_cost > di.InventoryUnitCost"""
    conn.execute(insert_query + select_query)

    tpt_cost_warning = """SELECT DISTINCT 'I_TransportationPolicy',
                                        '(FromLocationId, ToLocationId, ItemCode, FromUnitCost, ToUnitCost)',
                                        '('||dt.FromLocationId||','||dt.ToLocationId||','||dt.ItemId||','||src.InventoryUnitCost||','||dest.InventoryUnitCost||')',
                                        'Warning',
                                        'Upstream cost is lower than downstream in Distribution'
                        FROM I_TransportationPolicy dt,
                            I_InventoryPolicy src,
                            I_InventoryPolicy dest
                        WHERE dt.FromLocationId = src.LocationId
                        and   dt.ItemId = src.ItemId
                        and   dt.ToLocationId = dest.LocationId
                        and   dt.ItemId = dest.ItemId
                        and   src.InventoryUnitCost > dest.InventoryUnitCost"""
    conn.execute(insert_query + tpt_cost_warning)


def split_ratio_validation(conn):
    ''' Check sum of MinSplitRatio in I_TransportationPolicy table and I_Processes table 
        should not be greater than 1 '''
    select_errors = """SELECT 'I_Processes',
                        '(ItemId, LocationId, ProcessId)', 
                            '('||ItemId||','||LocationId||','||ProcessId||')',
                            'Error',
                            'Different split ratio across process steps'
                        FROM I_Processes
                        WHERE MinSplitRatio is NOT NULL
                        GROUP BY ItemId,
                                LocationId,
                                ProcessId
                        HAVING Min(ifnull(MinSplitRatio, 0)) != Max(ifnull(MinSplitRatio, 0))"""
    conn.execute(insert_query + select_errors)


    
    op_split_error = """SELECT 'I_Processes',
                                '(ItemId, LocationId)',
                                '(' || ItemId || ',' || LocationId || ')',
                                'Error',
                                'Sum of MinSplitRatio should be less than 1'
                            FROM (
                                    SELECT ItemId,
                                            LocationId,
                                            ProcessId,
                                            Max(MinSplitRatio) AS MinSplitRatio
                                        FROM I_Processes
                                        WHERE MinSplitRatio IS NOT NULL
                                        GROUP BY ItemId,
                                                LocationId,
                                                ProcessId
                                )
                            GROUP BY ItemId,
                                    LocationId
                            HAVING ROUND(SUM(MinSplitRatio),9) > 1"""
    
    create_temp_table = """CREATE TABLE TEMP.OP_SPLIT_ERRORS AS
                            SELECT DISTINCT ItemID, LocationId
                            FROM (
                                    SELECT ItemId,
                                            LocationId,
                                            ProcessId,
                                            Max(MinSplitRatio) AS MinSplitRatio
                                        FROM I_Processes
                                        WHERE MinSplitRatio IS NOT NULL
                                        GROUP BY ItemId,
                                                LocationId,
                                                ProcessId
                                )
                            GROUP BY ItemId,
                                    LocationId
                            HAVING ROUND(SUM(MinSplitRatio),9) > 1"""

    conn.execute(insert_query + op_split_error)
    conn.execute(create_temp_table)
    
    row_check = """select 1 from I_ProcessesPerPeriod
                    where MinSplitRatio is not null"""
    rows = conn.execute(row_check).fetchone()
    if rows:
        multi_period_query = """SELECT 'I_ProcessesPerPeriod',
                                '(ItemId, LocationId, PeriodStart)', 
                                '('||ItemId||','||LocationId||','||date(PeriodStart + julianday('1899-12-30'))||')',
                                'Error',
                                'Sum of MinSplitRatio should not be more than 1'
                        FROM 
                        (
                            SELECT I_Processes.ItemId,
                                I_Processes.LocationId,
                                I_Processes.ProcessId,
                                dpx.PeriodStart,
                                max(ifnull(opp.MinSplitRatio, I_Processes.MinSplitRatio)) as MaxMinSplitRatio,
                                min(ifnull(opp.MinSplitRatio, I_Processes.MinSplitRatio)) as MinMinSplitRatio
                            from I_Processes,
                                O_Period dpx
                            LEFT JOIN I_ProcessesPerPeriod opp
                            ON I_Processes.ItemId = opp.ItemId
                            AND I_Processes.LocationId = opp.LocationId
                            AND I_Processes.ProcessId = opp.ProcessId
                            and I_Processes.PRocessStep= opp.ProcessStep
                            and dpx.PEriodStart = opp.StartDate
                            LEFT JOIN TEMP.OP_SPLIT_ERRORS tosp
                            ON I_Processes.ItemId = tosp.ItemId
                            AND I_Processes.LocationId = tosp.LocationId
                            WHERE  ifnull(opp.MinSplitRatio, I_Processes.MinSplitRatio) IS NOT NULL
                            AND    tosp.ItemId is null
                            GROUP BY  I_Processes.ItemId,
                                    I_Processes.LocationId,
                                    I_Processes.ProcessId,
                                    dpx.PeriodStart
                        )
                        GROUP BY ItemId, LocationId, PEriodStart
                        HAVING round(sum(MaxMinSplitRatio),5) > 1
                        OR round(sum(MinMinSplitRatio),5) > 1"""
        conn.execute(insert_query + multi_period_query)

    tp_split_query = """select  'I_TransportationPolicy',
                                    '(ItemId, ToLocationId)',
                                    '('||ItemId||','||ToLocationId||')' , 
                                    'LTSP Error',
                                    'Sum of MinSplitRatio should be less than 1'
                            from I_TransportationPolicy
                            WHERE MinSplitRatio IS NOT NULL
                            GROUP BY ItemId, ToLocationId
                            HAVING    ROUND(Sum(MinSplitRatio),9) > 1"""
    conn.execute(insert_query + tp_split_query)

    create_temp_table = """CREATE TABLE TEMP.TP_SPLIT_ERRORS AS
                            SELECT ItemID, ToLocationId
                            From I_TransportationPolicy
                            WHERE MinSplitRatio IS NOT NULL
                            GROUP BY ItemId, ToLocationId
                            HAVING    ROUND(Sum(MinSplitRatio),9) > 1"""

    conn.execute(create_temp_table)

    row_check = """select 1 from I_TransportationPolicyPerPeriod
                    where MinSplitRatio is not null"""
    rows = conn.execute(row_check).fetchone()
    if rows:
        tp_split_query = """select  'I_TransportationPolicyPerPeriod',
                            '(ItemId, ToLocationId, PeriodStart)',
                            '('||dt.ItemId||','||dt.ToLocationId||','||date(dp.PeriodStart + julianday('1899-12-30'))||')' , 
                            'Error',
                            CASE WHEN ROUND(SUM(ifnull(dtp.MinSplitRatio, 0)),3) <= 1 THEN
                                'Split factors information incomplete in I_TransportationPolicyPerPeriod vs. I_TransportationPolicy'
                                ELSE 'Sum of MinSplitRatio should be less than 1' END
                    FROM I_TransportationPolicy dt,
                        O_Period dp
                    LEFT JOIN I_TransportationPolicyPerPeriod dtp
                    ON dt.ItemId = dtp.ItemId
                    AND dt.FromLocationId = dtp.FromLocationId
                    AND dt.ToLocationId = dtp.ToLocationId
                    AND dt.ModeId = dtp.ModeId
                    AND dp.PeriodStart = dtp.StartDate
                    LEFT JOIN TEMP.TP_SPLIT_ERRORS ttsp
                    ON dt.ItemId = ttsp.ItemId
                    AND dt.ToLocationId = ttsp.ToLocationId
                    WHERE ifnull(dtp.MinSplitRatio, dt.MinSplitRatio) is not null
                    and   ttsp.ToLocationId is null
                    GROUP BY dt.ItemId, dt.ToLocationId, dp.PeriodStart
                    HAVING    ROUND(SUM(ifnull(dtp.MinSplitRatio, dt.MinSplitRatio)),9) > 1"""
        conn.execute(insert_query + tp_split_query)

    zero_production = """SELECT 'I_TransportationPolicyPerPeriod', 
                        '(ItemId, FromLocationId, ToLocationId, PeriodStart)',
                        '('||t1.ItemId||','||t1.LocationId||','||t1.ToLocationId||','||date(t1.PeriodStart + julianday('1899-12-30'))||')' , 
                        'Warning',
                        '0 MaxProduction but non zero MinSplitRatio from this location'
                    FROM
                    (
                    SELECT DISTINCT dt.ItemId,
                        dt.FromLocationId LocationId,
                        dt.ToLocationId,
                        dp.PeriodStart,
                        ifnull(ifnull(dtp.MinSplitRatio, dt.MinSplitRatio),0) as split_ratio
                    FROM I_TransportationPolicy dt,
                        O_Period dp
                    LEFT JOIN I_TransportationPolicyPerPeriod dtp
                    ON dt.ItemId = dtp.ItemId
                    AND dt.FromLocationId = dtp.FromLocationId
                    AND dt.ToLocationId = dtp.ToLocationId
                    AND dt.ModeId = dtp.ModeId
                    AND dp.PeriodStart = dtp.StartDate
                    WHERE ifnull(ifnull(dtp.MinSplitRatio, dt.MinSplitRatio),0) > 0
                    ) t1,
                    (
                    SELECT DISTINCT di.ItemId,
                        di.LocationId,
                        dp.PeriodStart
                    FROM I_InventoryPolicy di,
                        O_Period dp
                    LEFT JOIN I_InventoryPolicyPerPeriod dip
                    ON di.ItemId = dip.ItemId
                    AND di.LocationId = dip.LocationId
                    AND dp.PeriodStart = dip.StartDate
                    WHERE ifnull(ifnull(dip.MaxProductionQuantity, di.MaxProductionQuantity),'100')  = '0'
                    ) t2
                    WHERE t1.ItemId = t2.ItemId
                    and   t1.LocationId = t2.LocationId
                    and   t1.PeriodStart = t2.PEriodStart"""
    conn.execute(insert_query + zero_production)

    insert_log(conn, f"{'-'* 5} Split ratios validated {'-'* 5}")