from ..sql_connector import insert_log

update_inventory_sp = """UPDATE I_InventoryPolicy
                        set SalesPrice = I_ItemMaster.SalesPrice
                        FROM I_ItemMaster
                        WHERE I_InventoryPolicy.ItemId = I_ItemMaster.ItemId
                        and   I_ItemMaster.SalesPrice IS NOT NULL
                        AND   I_InventoryPolicy.SalesPrice IS NULL"""

update_inventory_cost = """UPDATE I_InventoryPolicy
                        set InventoryUnitCost = I_ItemMaster.UnitCost
                        FROM I_ItemMaster
                        WHERE I_InventoryPolicy.ItemId = I_ItemMaster.ItemId
                        and   I_ItemMaster.UnitCost IS NOT NULL
                        AND   I_InventoryPolicy.InventoryUnitCost IS NULL"""

update_forecast_sp = """UPDATE I_ForecastOrders
                        set SalesPrice = I_InventoryPolicy.SalesPrice
                        FROM I_InventoryPolicy
                        WHERE I_ForecastOrders.ItemId = I_InventoryPolicy.ItemId
                        AND   I_ForecastOrders.LocationId = I_InventoryPolicy.LocationId
                        AND   I_InventoryPolicy.SalesPrice IS NOT NULL
                        AND   I_ForecastOrders.SalesPrice IS NULL"""

default_data = {'I_InventoryPolicy': {
                                'MinEndingInventory': 0,
                                'MaxEndingInventory': 'INF',
                                'MinProductionQuantity': 0,
                                'MaxProductionQuantity': 'INF',
                                'SafetyStockDOS': 0,
                                'DOSWindow': 0,
                                'InventoryStatus': 1
                                },
                'I_ItemMaster':   {
                                'ItemStatus': 1
                                },
                'I_Processes': {
                                'Yield': 1
                                },
                'I_ResourceMaster': {
                                'SupplyCapacity': 'INF',
                                'MinUtilization': 0,
                                'MaxUtilization': 1
                                }
                            }

def update_defaults(conn):
    ''' populate default values based on default_data object 
        1. It populates UnicostModel in I_InventoryPolicy table if there are nulls and 
            there is corresponding not null record in I_ItemMaster table
        2. It populates SalesPrice column in I_InventoryPolicy table, if there are nulls and there is corresponding
            not null record in I_ItemMaster table
        3. It populates SalesPrice column in I_ForecastOrders table, if there are nulls and 
            there is corresponding not null record in I_InventoryPolicy table
    '''
    for table_name in default_data:
        for column_name in default_data[table_name]:
            default_value = (default_data[table_name][column_name],)
            query = f"""UPDATE [{table_name}]
                        SET [{column_name}] = ?
                        WHERE [{column_name}] IS NULL"""
            conn.execute(query, default_value)
    conn.execute(update_inventory_cost)
    conn.execute(update_inventory_sp)
    conn.execute(update_forecast_sp)
    insert_log(conn, f"{'-'* 5} Populate default completed {'-'* 5}")