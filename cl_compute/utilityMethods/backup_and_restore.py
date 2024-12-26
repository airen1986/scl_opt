from ..sql_connector import insert_log

def backup_tables(conn):
    ''' Few tables are populated with default values for model validation, so backup of such tables is taken 
        which will then be restored after model validation '''
    conn.execute("DROP TABLE IF EXISTS XT_InventoryPolicy")
    conn.execute("DROP TABLE IF EXISTS XT_ForecastOrders")
    conn.execute("DROP TABLE IF EXISTS XT_Processes")
    conn.execute("DROP TABLE IF EXISTS XT_TransportationPolicy")
    conn.execute("CREATE TABLE XT_InventoryPolicy as SELECT * FROM I_InventoryPolicy")
    conn.execute("CREATE TABLE XT_ForecastOrders as SELECT * FROM I_ForecastOrders")
    conn.execute("CREATE TABLE XT_TransportationPolicy as SELECT * FROM I_TransportationPolicy")
    conn.execute("CREATE TABLE XT_Processes as SELECT * FROM I_Processes")
    insert_log(conn, f"{'-'* 5} Table backup completed {'-'* 5}")

def restore_tables(conn):
    conn.execute("DELETE FROM I_InventoryPolicy")
    conn.execute("INSERT INTO I_InventoryPolicy SELECT * FROM XT_InventoryPolicy")
    conn.execute("DELETE FROM I_ForecastOrders")
    conn.execute("INSERT INTO I_ForecastOrders SELECT * FROM XT_ForecastOrders")
    conn.execute("DELETE FROM I_Processes")
    conn.execute("INSERT INTO I_Processes SELECT * FROM XT_Processes")
    conn.execute("DELETE FROM I_TransportationPolicy")
    conn.execute("INSERT INTO I_TransportationPolicy SELECT * FROM XT_TransportationPolicy")
    conn.execute("DROP TABLE XT_InventoryPolicy")
    conn.execute("DROP TABLE XT_ForecastOrders")
    conn.execute("DROP TABLE XT_Processes")
    conn.execute("DROP TABLE XT_TransportationPolicy")
    insert_log(conn, f"{'-'* 5} Table restore completed {'-'* 5}")
