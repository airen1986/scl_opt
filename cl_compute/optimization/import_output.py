from .output_queries import *
from .queries import ss_sql
from ..sql_connector import insert_log

def round_fn(val):
    if val is None:
        return 0
    else:
        return round(val, 5)
    

def get_val(val):
    if val is None:
        return 0
    if hasattr(val, "varValue"):
        if val.varValue is None:
            return 0
        return val.varValue
    return val

def main(conn, demand_dict, inventory_dict, reg_demand_dict, process_dict, 
         flow_dict, inbound_flow, dem_shortfall, inv_shortfall, all_combinations, periods, 
         initial_inv, bom_var, expiry_var):
    delete_output_tables(conn)
    insert_log(conn, "Output import started")

    ss_requirement = {}
    query = "select DOSWindowStartPeriod from I_ModelSetup"
    n = conn.execute(query).fetchone()[0]

    for item, location, period, prd_idx, dos_window, ss_req in conn.execute(ss_sql):
        dos_periods = periods[prd_idx+n: prd_idx+n+int(dos_window)]
        ct = int(dos_window)
        if ct == 0:
            continue
        out_flow = sum(get_val(flow_dict[item][location][to_location][mode][period])
                            for to_location in flow_dict.get(item, {}).get(location, {})
                            for mode in flow_dict[item][location][to_location]
                            for period in flow_dict[item][location][to_location][mode]
                            if period in dos_periods)/ct + \
                    sum(get_val(demand_dict[item][location][period]['var'])
                            for period in demand_dict.get(item, {}).get(location, {})
                            if period in dos_periods)/ct + \
                    sum(get_val(bom_var[item][location][period])
                            for period in bom_var.get(item, {}).get(location, {})
                            if period in dos_periods)/ct + \
                    sum(get_val(reg_demand_dict[item][location][period][demand_item])
                            for period in dos_periods 
                            for demand_item in reg_demand_dict.get(item, {}).get(location, {}).get(period, {}))/ct
        req = round(ss_req * out_flow, 5)
        if item not in ss_requirement:
            ss_requirement[item] = {location: {period: req}}
        elif location not in ss_requirement[item]:
            ss_requirement[item][location] = {period: req}
        else:
            ss_requirement[item][location][period] = req


    for item, location in all_combinations:
        for period_idx, period in enumerate(periods):
            opening_inv = get_val(initial_inv.get(item, {}).get(location, {}).get(period, 0))
            incoming_stock = 0
            if period_idx > 0:
                incoming_stock = opening_inv
                opening_inv += get_val(inventory_dict.get(item, {}).get(location, {}).get(periods[period_idx-1], 0))
            closing_inv = get_val(inventory_dict.get(item, {}).get(location, {}).get(period, 0))
            prod_qty = get_val(sum(process_dict[item][location][process][period].varValue
                                    for process in process_dict.get(item, {}).get(location, {})
                                    if period in process_dict[item][location][process]))
            shortfall_qty = get_val(inv_shortfall.get(item, {}).get(location, {}).get(period, 0))
            inbound_qty = sum(get_val(inbound_flow[item][from_location][location][mode][period])
                                    for from_location in inbound_flow.get(item, {})
                                    if location in inbound_flow[item][from_location]
                                    for mode in inbound_flow[item][from_location][location]
                                    if period in inbound_flow[item][from_location][location][mode])
            
            outbound_qty = get_val(sum(flow_dict[item][location][to_location][mode][period].varValue
                                        for to_location in flow_dict.get(item, {}).get(location, {})
                                        for mode in flow_dict[item][location][to_location]
                                        if period in flow_dict[item][location][to_location][mode]))
            satisfied_qty = get_val(demand_dict.get(item, {}).get(location, {}).get(period, 
                                                                                 {}).get('var', 0))
            demand_qty = get_val(demand_dict.get(item, {}).get(location, {}).get(period, 
                                                                                 {}).get('max', 0))
            reg_inbound = get_val(sum(reg_demand_dict[reg_item][location][period][item].varValue
                                    for reg_item in reg_demand_dict
                                    if location in reg_demand_dict[reg_item]
                                    if period in reg_demand_dict[reg_item][location]
                                    if item in reg_demand_dict[reg_item][location][period]))
            reg_outbound = get_val(sum(reg_demand_dict[item][location][period][demand_item].varValue
                                    for demand_item in reg_demand_dict.get(item, 
                                            {}).get(location, {}).get(period, {})))
            consumed_qty = get_val(bom_var.get(item, {}).get(location, {}).get(period, 0))
            expired_qty = get_val(sum(var.varValue for var in 
                                expiry_var.get(item, {}).get(location, {}).get(period, [])))
            required_inventory = ss_requirement.get(item, {}).get(location, {}).get(period, 0)
            opening_inv = round(opening_inv, 5)
            closing_inv = round(closing_inv, 5)
            shortfall_qty = round(shortfall_qty, 5)
            inbound_qty = round(inbound_qty, 5)
            outbound_qty = round(outbound_qty, 5)
            prod_qty = round(prod_qty, 5)
            satisfied_qty = round(satisfied_qty, 5)
            demand_qty = round(demand_qty, 5)
            reg_inbound = round(reg_inbound, 5)
            reg_outbound = round(reg_outbound, 5)
            consumed_qty = round(consumed_qty, 5)
            required_inventory = round(required_inventory, 5)
            incoming_stock = round(incoming_stock, 5)
            insert_tpl = (item, location, period, opening_inv, closing_inv, shortfall_qty, 
                          inbound_qty, outbound_qty, prod_qty, satisfied_qty, demand_qty, reg_inbound, 
                         reg_outbound, consumed_qty, required_inventory, incoming_stock, expired_qty)
            conn.execute(inventory_sql, insert_tpl)
    
    for item in reg_demand_dict:
        for location in reg_demand_dict[item]:
            for period in reg_demand_dict[item][location]:
                for demand_item in reg_demand_dict[item][location][period]:
                    demand_qty = round_fn(reg_demand_dict[item][location][period][demand_item].varValue)
                    if demand_qty > 0:
                        insert_tpl = (item, location, period, demand_item, demand_qty)
                        conn.execute(reg_sql, insert_tpl)
                        
    for item in process_dict:
        for location in process_dict[item]:
            for process in process_dict[item][location]:
                for period in process_dict[item][location][process]:
                    prod_qty = round_fn(process_dict[item][location][process][period].varValue)
                    if prod_qty > 0:
                        insert_tpl = (item, location, process, period, prod_qty)
                        conn.execute(production_sql, insert_tpl)

    for item in flow_dict:
        for from_location in flow_dict[item]:
            for to_location in flow_dict[item][from_location]:
                for mode in flow_dict[item][from_location][to_location]:
                    for period in flow_dict[item][from_location][to_location][mode]:
                        flow_qty = round_fn(flow_dict[item][from_location][to_location][mode][period].varValue)
                        if flow_qty > 0:
                            insert_tpl = (item, from_location, to_location, period, mode, flow_qty)
                            conn.execute(flow_sql, insert_tpl)

    import_initial_inv(conn, initial_inv, periods[0])
    insert_log(conn, f"Output import completed ")
    conn.execute(update_period_end)
    conn.execute(update_intransit_inventory)
    conn.execute(update_ordered_quantity)
    conn.execute(update_production_cost)
    conn.execute(update_transportation_cost)
    conn.execute(update_inrelease_inventory)
    insert_log(conn, f"Output post processing completed")


def import_initial_inv(conn, initial_inv, initial_period):
    insert_log(conn, f"Initial inventory import started")
    for item in initial_inv:
        for location in initial_inv[item]:
            stock = 0
            var = initial_inv[item][location].get(initial_period, 0)
            var_val = get_val(var)
            stock += var_val
            stock = round(stock, 5)
            if stock > 0:
                conn.execute(initial_inv_sql, (item, location, stock))
    insert_log(conn, f"Initial inventory import completed")

def delete_output_tables(conn):
    output_tables = ("O_Inventory", "O_Transportation", "O_InitialInventory", 
                     "O_Objective", "O_Production", "O_ForecastRegistration")
    for table_name in output_tables:
        delete_query = f"DELETE FROM [{table_name}]"
        conn.execute(delete_query)

def update_objective(conn, solve_status, objective_name, objective_value):
    query = """INSERT INTO O_Objective (SolveStatus, ObjectiveName, ObjectiveValue)
               Values (?, ?, ?)"""
    insert_tpl = (solve_status, objective_name, str(objective_value))
    conn.execute(query, insert_tpl)
    insert_log(conn, ': '.join(insert_tpl))