from pulp import LpVariable, LpContinuous, lpSum, LpInteger
from .queries import *

def generate_demand_var(conn):
    demand_dict = {}
    for item, location, period, qty, sp in conn.execute(get_demand_sql):
        var = LpVariable(name=f"dem_{item}_{location}_{period}", lowBound=0, upBound=qty, cat=LpContinuous)
        if item not in demand_dict:
            demand_dict[item] = {location: {period: {'var': var, 'max': qty, 'sp': sp}}}
        elif location not in demand_dict[item]:
            demand_dict[item][location] = {period: {'var': var, 'max': qty, 'sp': sp}}
        else:
            demand_dict[item][location][period] = {'var': var, 'max': qty, 'sp': sp}
    return demand_dict

def get_demand_shortfall_var (prob, demand_dict, periods, product_values):
    new_prd = list(periods)
    new_prd.reverse()
    shortfall_var = {}
    for demand_item in demand_dict:
        shortfall_var[demand_item] = {}
        for location in demand_dict[demand_item]:
            shortfall_var[demand_item][location] = {}
            for period in demand_dict[demand_item][location]:
                var = LpVariable(name=f"shortfall_{demand_item}_{location}_{period}", 
                                 lowBound=0, upBound=None, cat=LpContinuous)
                shortfall_var[demand_item][location][period] = var
                prob += demand_dict[demand_item][location][period]['var'] + \
                        var == demand_dict[demand_item][location][period]['max'], \
                        f'demand_sat_{demand_item}_{location}_{period}'
                
    shortfall_objective = lpSum(shortfall_var[item][location][period] * (new_prd.index(period)+1) *
                                (product_values[item][location] if product_values[item][location] > 0 else 1)
                          for item in shortfall_var
                          for location in shortfall_var[item]
                          for period in shortfall_var[item][location])
    return shortfall_var, shortfall_objective

def generate_flow_var(prob, conn, periods, holding_cost):
    objective_expr = lpSum(0)
    flow_dict = {}
    split_ratio_dict = {}
    inbound_flow = {}
    period_len = len(periods)
    for item, from_location, to_location, mode, period, tpt_cost, \
        lead_time, min_qty, max_qty, min_ratio, max_ratio in conn.execute(get_flow_sql):
        if max_qty == 'INF':
            max_qty = None
        else:
            max_qty = float(max_qty)
        var = LpVariable(name=f"flow_{item}_{from_location}_{to_location}_{mode}_{period}", 
                         lowBound=min_qty, upBound=max_qty, cat=LpContinuous)
        if item not in flow_dict:
            flow_dict[item] = {from_location: {to_location: {mode: {period: var}}}}
            inbound_flow[item] = {from_location: {to_location: {mode: {period: 0}}}}
        elif from_location not in flow_dict[item]:
            flow_dict[item][from_location] = {to_location: {mode: {period: var}}}
            inbound_flow[item][from_location] = {to_location: {mode: {period: 0}}}
        elif to_location not in flow_dict[item][from_location]:
            flow_dict[item][from_location][to_location] = {mode: {period: var}}
            inbound_flow[item][from_location][to_location] = {mode: {period: 0}}
        elif mode not in flow_dict[item][from_location][to_location]:
            flow_dict[item][from_location][to_location][mode] = {period: var}
            inbound_flow[item][from_location][to_location][mode] = {period: 0}
        else:
            flow_dict[item][from_location][to_location][mode][period] = var
            if period not in inbound_flow[item][from_location][to_location][mode]:
                inbound_flow[item][from_location][to_location][mode][period] = 0
        if tpt_cost > 0:
            objective_expr += tpt_cost * var
        
        lead_time_int = int(round(lead_time,0))

        prd_idx = periods.index(period) + lead_time_int
        if prd_idx < period_len:
            recv_period = periods[prd_idx]
            inbound_flow[item][from_location][to_location][mode][recv_period] = var
        
        if lead_time_int > 0:
            objective_expr += holding_cost.get(item, {}).get(to_location, 0) * var * lead_time_int

        if max_ratio == 'INF':
            max_ratio = None
        else:
            max_ratio = float(max_ratio)

        if min_ratio > 0 or max_ratio is not None:
            if item not in split_ratio_dict:
                split_ratio_dict[item] = {from_location: {to_location: {mode: {period: (min_ratio, max_ratio)}}}}
            elif from_location not in split_ratio_dict[item]:
                split_ratio_dict[item][from_location] = {to_location: {mode: {period: (min_ratio, max_ratio)}}}
            elif to_location not in split_ratio_dict[item][from_location]:
                split_ratio_dict[item][from_location][to_location] = {mode: {period: (min_ratio, max_ratio)}}
            elif mode not in split_ratio_dict[item][from_location][to_location]:
                split_ratio_dict[item][from_location][to_location][mode] = {period: (min_ratio, max_ratio)}
            else:
                split_ratio_dict[item][from_location][to_location][mode][period] = (min_ratio, max_ratio)

    prob = add_tp_split_ratio_constraint(prob, split_ratio_dict, flow_dict)

    return prob, flow_dict, objective_expr, inbound_flow


def add_tp_split_ratio_constraint(prob, split_ratio_dict, flow_dict):
    # Mode is considered to be part of split ratio
    for item in split_ratio_dict:
        for from_location in split_ratio_dict[item]:
            for to_location in split_ratio_dict[item][from_location]:
                for mode in split_ratio_dict[item][from_location][to_location]:
                    for period in split_ratio_dict[item][from_location][to_location][mode]:
                        min_ratio, max_ratio = split_ratio_dict[item][from_location][to_location][mode][period]
                        if min_ratio > 0:
                            prob += flow_dict[item][from_location][to_location][mode][period] >= min_ratio * \
                                    lpSum(flow_dict[item][from_location_x][to_location][mode_x][period]
                                          for from_location_x in flow_dict[item]
                                          if to_location in flow_dict[item][from_location_x]
                                          for mode_x in flow_dict[item][from_location_x][to_location]
                                          if period in flow_dict[item][from_location_x][to_location][mode_x]), \
                                    f"min_tp_ratio_{item}_{from_location}_{to_location}_{mode}_{period}"
                        if max_ratio is not None:
                            prob += flow_dict[item][from_location][to_location][mode][period] <= max_ratio * \
                                    lpSum(flow_dict[item][from_location_x][to_location][mode_x][period]
                                          for from_location_x in flow_dict[item]
                                          if to_location in flow_dict[item][from_location_x]
                                          for mode_x in flow_dict[item][from_location_x][to_location]
                                          if period in flow_dict[item][from_location_x][to_location][mode_x]), \
                                    f"max_tp_ratio_{item}_{from_location}_{to_location}_{mode}_{period}"
    return prob


def generate_inventory_var(conn):
    objective_expr = lpSum(0)
    inventory_dict = {}
    product_values = {}
    holding_cost_dict = {}
    for item, location, period, min_inventory, max_inventory, \
        holding_cost, product_value in conn.execute(get_inventory_sql):
        if max_inventory == 'INF':
            max_inventory = None
        else:
            max_inventory = float(max_inventory)
        min_inventory = float(min_inventory)
        if min_inventory == max_inventory:
            var = min_inventory
        else:
            var = LpVariable(name=f"inv_{item}_{location}_{period}", lowBound=min_inventory, 
                            upBound=max_inventory, cat=LpContinuous)
        if item not in inventory_dict:
            inventory_dict[item] = {location: {period: var}}
            product_values[item] = {location: product_value}
            holding_cost_dict[item] = {location: holding_cost}
        elif location not in inventory_dict[item]:
            inventory_dict[item][location] = {period: var}
            product_values[item][location] = product_value
            holding_cost_dict[item][location] = holding_cost
        else:
            inventory_dict[item][location][period] = var
        objective_expr += var * holding_cost

    return inventory_dict,  objective_expr, product_values, holding_cost_dict


def get_master_data(conn):
    periods = []
    for row in conn.execute(get_periods_sql):
        periods.append(row[0])
    combinations =[]
    for item, location in conn.execute(get_combinations_sql):
        combinations.append((item, location))
    return periods, combinations


def get_production_var(conn):
    production_dict = {}
    for item, location, period, min_prod, max_prod in conn.execute(get_production_sql):
        if max_prod == 'INF':
            max_prod = None
        else:
            max_prod = float(max_prod)
        min_prod = float(min_prod)
        if max_prod == 0:
            var = 0
        else:
            var = LpVariable(name=f"production_{item}_{location}_{period}", lowBound=0, 
                            upBound=max_prod, cat=LpContinuous)
        if item not in production_dict:
            production_dict[item] = {location: {period: var}}
        elif location not in production_dict[item]:
            production_dict[item][location] = {period: var}
        else:
            production_dict[item][location][period] = var
    return production_dict


def demand_matching(prob, periods, inventory_dict, production_dict, initial_inv, flow_dict, 
                    demand_dict, all_combinations, bom_var, reg_demand_dict, inbound_dict, 
                    expiry_var):
    for period_no, period in enumerate(periods):
        for item, location in all_combinations:
            reg_out = lpSum(reg_demand_dict[item][location][period][demand_item] for 
                           demand_item in reg_demand_dict.get(item, {}).get(location, {}).get(period, {}))
            demand = demand_dict.get(item, {}).get(location, {}).get(period, {}).get('var', 0)
            reg_in = lpSum(reg_demand_dict[reg_item][location][period][item] 
                           for reg_item in reg_demand_dict
                           if location in reg_demand_dict.get(reg_item, {})
                           if period in reg_demand_dict.get(reg_item, {}).get(location, {})
                           if item in reg_demand_dict.get(reg_item, {}).get(location, {}).get(period, {}))
            bom_demand = bom_var.get(item, {}).get(location, {}).get(period, 0)
            outbound_flow = lpSum(flow_dict[item][location][to_location][mode][period]
                                    for to_location in flow_dict.get(item, {}).get(location, {})
                                    for mode in flow_dict[item][location][to_location]
                                    if period in flow_dict[item][location][to_location][mode])
            inbound_flow = lpSum(inbound_dict[item][from_location][location][mode][period]
                                    for from_location in inbound_dict.get(item, {})
                                    if location in inbound_dict[item][from_location]
                                    for mode in inbound_dict[item][from_location][location]
                                    if period in inbound_dict[item][from_location][location][mode])
            expired_qty = lpSum(expiry_var.get(item, {}).get(location, {}).get(period, [0]))
            beginning_inv = initial_inv.get(item, {}).get(location, {}).get(period, 0)
            if period_no > 0:
                beginning_inv += inventory_dict.get(item, {}).get(location, {}).get(periods[period_no-1], 0)

            ending_inv = inventory_dict.get(item, {}).get(location, {}).get(period, 0)
            production = production_dict.get(item, {}).get(location, {}).get(period, 0)

            prob += inbound_flow + beginning_inv + production + reg_in == \
                ending_inv + outbound_flow + demand + bom_demand + reg_out + expired_qty, \
                    f'demand_matching_{item}_{location}_{period}'
    return prob


def get_resource_var(conn):
    resource_dict = {}
    for resource, location, period, supply_capacity, min_util, \
        max_util in conn.execute(get_resources_sql):
        if supply_capacity == 'INF':
            min_prod = 0
            max_prod = None
        elif max_util == 'INF':
            min_prod = min_util * float(supply_capacity)
            max_prod = None
        else:
            min_prod = min_util * float(supply_capacity)
            max_prod = float(max_util) * float(supply_capacity)
        
        var = LpVariable(name=f"res_{resource}_{location}_{period}", lowBound=min_prod, 
                         upBound=max_prod, cat=LpContinuous)
        if resource not in resource_dict:
            resource_dict[resource] = {location: {period: var}}
        elif location not in resource_dict[resource]:
            resource_dict[resource][location] = {period: var}
        else:
            resource_dict[resource][location][period] = var

    return resource_dict



def get_process_var(prob, conn, production_dict):
    process_dict = {}
    split_ratio_dict = {}
    objective_expr = lpSum(0)
    for item, location, process, period, min_ratio, max_ratio, \
        prod_cost in conn.execute(get_processes_sql):
        var = LpVariable(name=f"process_{item}_{location}_{process}_{period}", lowBound=0, 
                         upBound=None, cat=LpContinuous)
        if item not in process_dict:
            process_dict[item] = {location: {process: {period: var}}}
        elif location not in process_dict[item]:
            process_dict[item][location] = {process: {period: var}}
        elif process not in process_dict[item][location]:
            process_dict[item][location][process] = {period: var}
        else:
            process_dict[item][location][process][period] = var

        objective_expr += prod_cost * var

        if max_ratio == 'INF':
            max_ratio = None
        else:
            max_ratio = float(max_ratio)

        if min_ratio > 0 or max_ratio is not None:
            if item not in split_ratio_dict:
                split_ratio_dict[item] = {location: {process: {period: (min_ratio, max_ratio)}}}
            elif location not in split_ratio_dict[item]:
                split_ratio_dict[item][location] = {process: {period: (min_ratio, max_ratio)}}
            elif process not in split_ratio_dict[item][location]:
                split_ratio_dict[item][location][process] = {period: (min_ratio, max_ratio)}
            else:
                split_ratio_dict[item][location][process][period] = (min_ratio, max_ratio)
    prob = add_process_split_ratio_constraint(prob, process_dict, split_ratio_dict)

    for item in production_dict:
        for location in production_dict[item]:
            for period in production_dict[item][location]:
                prob += production_dict[item][location][period] == \
                    lpSum(process_dict[item][location][process_x][period] 
                          for process_x in process_dict[item][location]
                          if period in process_dict[item][location][process_x]), \
                            f'process_prod_{item}_{location}_{period}'

    return prob, process_dict, objective_expr



def add_process_split_ratio_constraint(prob, process_dict, split_ratio_dict):
    for item in split_ratio_dict:
        for location in split_ratio_dict[item]:
            for process in split_ratio_dict[item][location]:
                for period in split_ratio_dict[item][location][process]:
                    min_ratio, max_ratio = split_ratio_dict[item][location][process][period]
                    if min_ratio > 0:
                        prob += process_dict[item][location][process][period] >= min_ratio * \
                            lpSum(process_dict[item][location][process_x][period] 
                                for process_x in process_dict[item][location]
                                if period in process_dict[item][location][process_x]), \
                                    f'pr_min_const_{item}_{location}_{process}_{period}'
                    if max_ratio is not None:
                        prob += process_dict[item][location][process][period] <= max_ratio * \
                            lpSum(process_dict[item][location][process_x][period] 
                                for process_x in process_dict[item][location]
                                if period in process_dict[item][location][process_x]), \
                                    f'pr_max_const_{item}_{location}_{process}_{period}'
    return prob



def get_bom_var(prob, conn, process_dict):
    bom_relation = {}
    for process, to_item, location, from_item, period, usage_qty, yiel_d in conn.execute(get_bom_sql):
        if from_item not in bom_relation:
            bom_relation[from_item] = {location: {period: {process: {to_item: usage_qty / yiel_d}}}}
        elif location not in bom_relation[from_item]:
            bom_relation[from_item][location] = {period: {process: {to_item: usage_qty / yiel_d}}}
        elif period not in bom_relation[from_item][location]:
            bom_relation[from_item][location][period] = {process: {to_item: usage_qty / yiel_d}}
        elif process not in bom_relation[from_item][location][period]:
            bom_relation[from_item][location][period][process] = {to_item: usage_qty / yiel_d}
        elif to_item not in bom_relation[from_item][location][period][process]:
            bom_relation[from_item][location][period][process][to_item] = usage_qty / yiel_d
        else:
            bom_relation[from_item][location][period][process][to_item] *= 1/yiel_d
    
    bom_var = {}
    for item in bom_relation:
        bom_var[item] = {}
        for location in bom_relation[item]:
            bom_var[item][location] = {}
            for period in bom_relation[item][location]:
                var = LpVariable(name=f"bom_{item}_{location}_{period}", lowBound=0, 
                         upBound=None, cat=LpContinuous)
                bom_var[item][location][period] = var
                prob += bom_var[item][location][period] == \
                    lpSum(process_dict.get(to_item,{}).get(location, {}).get(process, {}).get(period, 0) * \
                          bom_relation[item][location][period][process][to_item]
                          for process in bom_relation[item][location][period]
                          for to_item in bom_relation[item][location][period][process]), \
                          f'bom_constraint_{item}_{location}_{period}'
    return prob, bom_var
            

def get_reg_cal_demand(prob, conn, demand_dict):
    reg_demand_dict = {}
    for item, location, period, demand_item, qty in conn.execute(reg_demand_sql):
        if period not in demand_dict.get(demand_item, {}).get(location, {}):
            continue
        var = LpVariable(name=f"reg_dm_{item}_{location}_{period}_{demand_item}", lowBound=0, 
                        upBound=None, cat=LpContinuous)
        if item not in reg_demand_dict:
            reg_demand_dict[item] = {location: {period: {demand_item: var}}}
        elif location not in reg_demand_dict[item]:
            reg_demand_dict[item][location] = {period: {demand_item: var}}
        elif period not in reg_demand_dict[item][location]:
            reg_demand_dict[item][location][period] = {demand_item: var}
        else:
            reg_demand_dict[item][location][period][demand_item] = var

    return prob, reg_demand_dict

            

def get_initial_inventory(conn, initialize_inv, initial_period):
    initial_inv = {}
    objective_expr = lpSum(0)
    conn.execute(update_entry_date_sql)
    for item, location, period, initial_qty in conn.execute(initial_inv_sql):
        if item not in initial_inv:
            initial_inv[item] = {location: {period: initial_qty}}
        elif location not in initial_inv[item]:
            initial_inv[item][location] = {period: initial_qty}
        else:
            initial_inv[item][location][period] = initial_qty

    if initialize_inv:
        for item, location, unit_cost in conn.execute(get_stocking_locations_sql):
            min_inv = initial_inv.get(item, {}).get(location, {}).get(initial_period, 0)
            var = LpVariable(name=f"inv_{item}_{location}", lowBound=min_inv, 
                             upBound=None, cat=LpContinuous)
            objective_expr += unit_cost * var
            if item not in initial_inv:
                initial_inv[item] = {location: {initial_period: var}}
            elif location not in initial_inv[item]:
                initial_inv[item][location]= {initial_period: var}
            else:
                initial_inv[item][location][initial_period] = var

    return initial_inv, objective_expr


def get_expiry_variable(conn, prob, periods, flow_dict, demand_dict, bom_var, holding_cost):
    j = 1
    expiry_var_dict = {}
    expiry_holding_cost = lpSum(0)
    for item, location, entry_period, expiry_period, stock_qty in conn.execute(expiry_inv_sql):
        expiry_periods = periods[periods.index(entry_period): periods.index(expiry_period)]
        out_flow = lpSum(flow_dict[item][location][to_location][mode][period]
                            for to_location in flow_dict.get(item, {}).get(location, {})
                            for mode in flow_dict[item][location][to_location]
                            for period in flow_dict[item][location][to_location][mode]
                            if period in expiry_periods) + \
                    lpSum(demand_dict[item][location][period]['var']
                            for period in demand_dict.get(item, {}).get(location, {})
                            if period in expiry_periods) + \
                    lpSum(bom_var[item][location][period]
                            for period in bom_var.get(item, {}).get(location, {})
                            if period in expiry_periods)
        
        expired_var = LpVariable(name=f"res_{item}_{location}_{expiry_period}_{j}", 
                                 lowBound=0, upBound=stock_qty, cat=LpContinuous)
        if item not in expiry_var_dict:
            expiry_var_dict[item] = {location: {expiry_period: [expired_var]}}
        elif location not in expiry_var_dict[item]:
            expiry_var_dict[item][location] = {expiry_period: [expired_var]}
        elif expiry_period not in expiry_var_dict[item][location]:
            expiry_var_dict[item][location][expiry_period] = [expired_var]
        else:
            expiry_var_dict[item][location][expiry_period].append(expired_var)
        prob += expired_var + out_flow >= stock_qty, \
                    f'expiry_constraint_{item}_{location}_{j}'
        j = j + 1
        n = len(periods) - periods.index(expiry_period) + 1
        expiry_holding_cost += expired_var * n * holding_cost[item][location]

    return prob, expiry_var_dict, expiry_holding_cost