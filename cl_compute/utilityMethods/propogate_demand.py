from ..sql_connector import insert_log
from .populate_periods import main as populate_periods

bom_select_query = """SELECT t2.FromItemId, t2.ToItemId, t2.LocationId, 
                                sum(t2.usageQuantity * da.Quantity) as Quantity
                                FROM
                                (
                                SELECT t1.ItemId as ToItemId, t1.LocationId, 
                                db.ItemId as FromItemId, usageQuantity
                                FROM
                                (
                                    select  dop.ItemId, 
                                            dop.LocationId, 
                                            dop.BOMId,
                                            min(dop.rowid) as rid
                                    from I_Processes dop,
                                        I_InventoryPolicy di
                                    WHERE dop.BOMId is not null
                                    AND   di.ItemId = dop.ItemId
                                    and   di.LocationId = dop.LocationId
                                    GROUP BY dop.ItemId, dop.LocationId
                                ) as t1,
                                (
                                    select db.*
                                    from I_BOMRecipe db,
                                        I_InventoryPolicy di
                                    WHERE di.ItemId = db.ItemId
                                    and   di.LocationId = db.LocationId
                                ) db
                                WHERE t1.LocationId = db.LocationId
                                AND   t1.BOMId = db.BOMId
                                ) T2,
                                    O_DemandAnalysis da
                                WHERE t2.ToItemId = da.ItemId
                                and   t2.LocationId = da.LocationId
                                and   da.FulFilledQuantity = 0
                                GROUP BY t2.FromItemId, t2.ToItemId, t2.LocationId """

def main(conn):
    '''This method only works if there is only one sourcing option for each inventory'''
    ''' This method will first add demand based on D_ForecastOrderItem table and then add upstream 
    dependent demand based on propogate_distribution and propogate_BOM methods
    It will also maintain a fulfilled column in O_DemandAnalysis table to keep track of which 
    demands are already satisfied'''

    insert_log(conn, f"{'-'* 5} Demand Propagation Started {'-'* 5}")
    ct = check_for_loops(conn)
    if ct > 0:
        insert_log(conn, "Demand Propogation: Check for loops in validation")

    conn.execute("DELETE FROM O_DemandAnalysis")
    populate_periods(conn)
    query = """INSERT INTO O_DemandAnalysis (ItemId, LocationId, FulFilledQuantity, Quantity, Iteration)
                select df.ItemId, df.LocationId, 0,  Sum(df.Quantity), 1
                from I_ForecastOrders df,
                     O_Period dp,
                     I_InventoryPolicy di
                WHERE df.ForecastArrivalDate = dp.PeriodStart
                AND   df.ItemId = di.ItemId
                AND   df.LocationId = di.LocationId
                Group BY df.ItemId, df.LocationId"""
    conn.execute(query)
    propogate_distribution(conn)
    ct = 1
    while ct > 0:
        row = conn.execute(bom_select_query).fetchone()
        if row:
            propogate_BOM(conn)
            propogate_distribution(conn)
        else:
            ct = 0
    aggregate_demand_analysis(conn)
    insert_log(conn, f"{'-'* 5} Demand Propagation Completed {'-'* 5}")



def propogate_distribution(conn):
    '''This method adds item location demand in O_DemandAnalysis table based on upstream transportation policies'''
    ct = 1
    iteration_count = conn.execute("SELECT max(Iteration) from O_DemandAnalysis").fetchone()[0]
    if iteration_count is None:
        iteration_count = 0
    while ct > 0:
        conn.execute("DROP TABLE IF EXISTS temp.temp_v;")
        create_temp_table = """CREATE TABLE temp.temp_v
                                AS
                                SELECT I_TransportationPolicy.ItemId,
                                        I_TransportationPolicy.FromLocationId,
                                        I_TransportationPolicy.toLocationId,
                                        CAST(1 AS FLOAT)/ct As SplitFactor,
                                        da.Quantity
                                    FROM (
                                            SELECT ItemId,
                                                    LocationId,
                                                    Sum(Quantity) AS Quantity
                                                FROM O_DemandAnalysis
                                                WHERE O_DemandAnalysis.FulFilledQuantity = 0
                                                GROUP BY ItemId,
                                                        LocationId
                                        )
                                        da
                                        LEFT JOIN
                                        (
                                            SELECT t1.ItemId,
                                                   t1.FromLocationId,
                                                   t1.ToLocationId,
                                                   t4.ct
                                                FROM I_TransportationPolicy t1,
                                                    I_InventoryPolicy t2,
                                                    I_InventoryPolicy t3,
                                                    (select ItemId, ToLocationId, COUNT(DISTINCT FromLocationId) AS CT
                                                    from I_TransportationPolicy
                                                    GROUP BY ItemId, ToLocationId) t4
                                                WHERE t1.ItemId = t2.ItemId AND 
                                                    t1.ItemId = t3.ItemId AND 
                                                    t1.ItemId = t4.ItemId AND 
                                                    t1.FromLocationId = t2.LocationId AND 
                                                    t1.ToLocationId = t3.LocationId and
                                                    t1.ToLocationId = t4.ToLocationId
                                                GROUP BY t1.ItemId,
                                                         t1.FromLocationId,
                                                         t1.ToLocationId
                                        )
                                        I_TransportationPolicy ON da.ItemId = I_TransportationPolicy.ItemId AND 
                                                            da.LocationId = I_TransportationPolicy.ToLocationId
                                    WHERE I_TransportationPolicy.FromLocationId IS NOT NULL
                                    GROUP BY I_TransportationPolicy.ItemId,
                                            I_TransportationPolicy.FromLocationId,
                                            I_TransportationPolicy.toLocationId """
        conn.execute(create_temp_table)
        iteration_count += 1
        add_query = """UPDATE O_DemandAnalysis
                    set FulFilledQuantity = 1
                    FROM temp.temp_v t1
                    WHERE O_DemandAnalysis.ItemId = t1.ItemId
                    and   O_DemandAnalysis.LocationId = t1.toLocationId
                    and   O_DemandAnalysis.FulFilledQuantity = 0;
                    INSERT INTO O_DemandAnalysis (ItemId, LocationId, FulFilledQuantity,  Quantity, Iteration)
                    SELECT ItemId, FromLocationId, 0, SUM(SplitFactor * Quantity), ?
                    FROM temp.temp_v
                    GROUP BY ItemId, FromLocationId;"""

        conn.execute(add_query, (iteration_count,))
        ct = conn.execute("select changes()").fetchone()[0]


def propogate_BOM(conn):
    ct = 1
    '''This method adds item location demand in O_DemandAnalysis table based on upstream bill of materials'''
    iteration_count = conn.execute("SELECT max(Iteration) from O_DemandAnalysis").fetchone()[0]
    if iteration_count is None:
        iteration_count = 0
    while ct > 0:
        conn.execute("DROP TABLE IF EXISTS temp.temp_v;")
        create_temp_table = f"""CREATE TABLE temp.temp_v
                                as
                                {bom_select_query}"""
        conn.execute(create_temp_table)
        iteration_count += 1
        add_query = """UPDATE O_DemandAnalysis
                    set FulFilledQuantity = 1
                    FROM temp.temp_v t1
                    WHERE O_DemandAnalysis.ItemId = t1.ToItemId
                    and   O_DemandAnalysis.LocationId = t1.LocationId
                    and   O_DemandAnalysis.FulFilledQuantity = 0;
                    INSERT INTO O_DemandAnalysis (ItemId, LocationId, FulFilledQuantity,  Quantity, Iteration)
                    SELECT FromItemId, LocationId, 0,  SUM(Quantity), ?
                    FROM temp.temp_v
                    GROUP BY FromItemId, LocationId;"""

        conn.execute(add_query, (iteration_count,))
        ct = conn.execute("select changes()").fetchone()[0]


def aggregate_demand_analysis(conn):
    ''' This method aggregates record in O_DemandAnalysis table, so that there are no duplicates'''

    conn.execute("DROP TABLE IF EXISTS temp.temp_v;")
    
    query = """CREATE TABLE temp.temp_v
                AS
                select ItemId, LocationId,  sum(quantity) as Quantity, 
                max(FulFilledQuantity) as FulFilledQuantity, Max(Iteration) as Iteration
                from O_DemandAnalysis
                group by ItemId, LocationId
                HAVING sum(quantity) > 0"""
    
    conn.execute(query)

    conn.execute("DELETE FROM O_DemandAnalysis;")

    query = """Insert into O_DemandAnalysis(ItemId, LocationId, Quantity, 
                FulFilledQuantity, Iteration)
                select ItemId, LocationId, Quantity, FulFilledQuantity, Iteration
                FROM  temp.temp_v"""
    conn.execute(query)


def check_for_loops(conn):
    ''' Model validation has already been completed before running this method, this method will look
        for loops and will stop demand propagation if it finds any'''
    query = """select count(*)
                from O_ModelValidation
                WHERE ErrorType  = 'Error'
                AND   ErrorMsg in ('Please check circular relations in TransportationPolicy',  
                    'Please check circular relations in BOMRecipe')"""
    row = conn.execute(query).fetchone()
    return row[0]
