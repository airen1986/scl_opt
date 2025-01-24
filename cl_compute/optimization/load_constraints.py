from pulp import  lpSum, LpStatus, LpVariable, LpContinuous, HiGHS
from .queries import get_resource_constraint_sql, min_release_sql, ss_sql, get_production_sql
from .import_output import update_objective

def add_resource_constraint(prob, conn, process_dict, resource_dict):
    resource_relation = {}
    for process, step, item, location, period, resource, yld, \
        process_time in conn.execute(get_resource_constraint_sql):
        if resource not in resource_relation:
            resource_relation[resource] = {location: {period: {item: {process: {step: process_time/yld}}}}}
        elif location not in resource_relation[resource]:
            resource_relation[resource][location] = {period: {item: {process: {step: process_time/yld}}}}
        elif period not in resource_relation[resource][location]:
            resource_relation[resource][location][period] = {item: {process: {step: process_time/yld}}}
        elif item not in resource_relation[resource][location][period]:
            resource_relation[resource][location][period][item] = {process: {step: process_time/yld}}
        elif process not in resource_relation[resource][location][period][item]:
            resource_relation[resource][location][period][item][process] = {step: process_time/yld}
        else:
            resource_relation[resource][location][period][item][process][step] = process_time/yld
    
    for resource in resource_relation:
        for location in resource_relation[resource]:
            for period in resource_relation[resource][location]:
                prob += resource_dict[resource][location][period] == \
                    lpSum(resource_relation[resource][location][period][item][process][step] * \
                          process_dict.get(item, {}).get(location, {}).get(process, {}).get(period, 0)
                          for item in resource_relation[resource][location][period]
                          for process in resource_relation[resource][location][period][item]
                          for step in resource_relation[resource][location][period][item][process]), \
                          f'res_const_{resource}_{location}_{period}'
    return prob


def add_min_relase_time_constraint(conn, prob, periods, inventory_dict, flow_dict, demand_dict, 
                                   bom_var, initial_inv, reg_demand_dict):
    for item, location, prd_name, prd_idx, release_time in conn.execute(min_release_sql):
        release_time = int(round(release_time,0))
        release_periods = periods[prd_idx+1: prd_idx+1+release_time]
        reg_inbound = lpSum(reg_demand_dict[from_item][location][period][item]
                            for from_item in reg_demand_dict
                            for period in release_periods
                            if item in reg_demand_dict[from_item].get(location, {}).get(period, {}))
        inbound_stock = lpSum(initial_inv[item][location][period]
                              for period in initial_inv.get(item, {}).get(location, {})
                              if period in release_periods)
        out_flow = lpSum(flow_dict[item][location][to_location][mode][period]
                            for to_location in flow_dict.get(item, {}).get(location, {})
                            for mode in flow_dict[item][location][to_location]
                            for period in flow_dict[item][location][to_location][mode]
                            if period in release_periods) + \
                    lpSum(demand_dict[item][location][period]['var']
                            for period in demand_dict.get(item, {}).get(location, {})
                            if period in release_periods) + \
                    lpSum(bom_var[item][location][period]
                            for period in bom_var.get(item, {}).get(location, {})
                            if period in release_periods)
        prob += inventory_dict[item][location][prd_name] >= out_flow - reg_inbound - inbound_stock, \
                f'min_release_constraint_{item}_{location}_{prd_name}'

        if prd_idx == 0:
            for partial_release_time in range(release_time):
                release_periods = periods[prd_idx: prd_idx+partial_release_time]
                out_flow = lpSum(flow_dict[item][location][to_location][mode][period]
                                for to_location in flow_dict.get(item, {}).get(location, {})
                                for mode in flow_dict[item][location][to_location]
                                for period in flow_dict[item][location][to_location][mode]
                                if period in release_periods) + \
                        lpSum(demand_dict[item][location][period]['var']
                                for period in demand_dict.get(item, {}).get(location, {})
                                if period in release_periods) + \
                        lpSum(bom_var[item][location][period]
                                for period in bom_var.get(item, {}).get(location, {})
                                if period in release_periods)
                reg_inbound = lpSum(reg_demand_dict[from_item][location][period][item]
                                for from_item in reg_demand_dict
                                for period in release_periods
                                if item in reg_demand_dict[from_item].get(location, {}).get(period, {}))
                inbound_stock = lpSum(initial_inv[item][location][period]
                                    for period in initial_inv.get(item, {}).get(location, {})
                                    if period in release_periods)
                prob += inbound_stock >= out_flow - reg_inbound, \
                    f'min_release_constraint_{item}_{location}_{prd_name}_{partial_release_time}'
    return prob


def minimize_inventory_shortfall(prob, conn, inventory_dict, flow_dict, periods, demand_dict, bom_var, 
                                 product_values, reg_demand_dict):
    inv_shortfall = {}
    for item in inventory_dict:
        inv_shortfall[item] = {}
        for location in inventory_dict[item]:
            inv_shortfall[item][location] = {}
            for period in inventory_dict[item][location]:
                var = LpVariable(name=f"inv_shortfall_{item}_{location}_{period}", 
                                 lowBound=0, upBound=None, cat=LpContinuous)
                inv_shortfall[item][location][period] = var

    query = "select ifnull(DOSWindowStartPeriod,1) from I_ModelSetup"
    n = conn.execute(query).fetchone()[0]
    
    optimize = False
    for item, location, period, prd_idx, dos_window, ss_req in conn.execute(ss_sql):
        optimize = True
        dos_periods = periods[prd_idx+n: prd_idx+n+int(dos_window)]
        ct = int(dos_window)
        if ct == 0:
            continue
        out_flow = lpSum(flow_dict[item][location][to_location][mode][period]
                            for to_location in flow_dict.get(item, {}).get(location, {})
                            for mode in flow_dict[item][location][to_location]
                            for period in flow_dict[item][location][to_location][mode]
                            if period in dos_periods)/ct + \
                    lpSum(demand_dict[item][location][period]['var']
                            for period in demand_dict.get(item, {}).get(location, {})
                            if period in dos_periods)/ct + \
                    lpSum(bom_var[item][location][period]
                            for period in bom_var.get(item, {}).get(location, {})
                            if period in dos_periods)/ct + \
                    lpSum(reg_demand_dict[item][location][period][demand_item]
                            for period in dos_periods 
                            for demand_item in reg_demand_dict.get(item, {}).get(location, {}).get(period, {}))/ct
        prob += inventory_dict[item][location][period] + inv_shortfall[item][location][period] \
            >= ss_req * out_flow, f'min_ss_constraint_{item}_{location}_{period}'
    if not optimize:
        return prob, inv_shortfall
    prob_objective = prob.objective
    inv_shortfall_objective = lpSum(inv_shortfall[item][location][period] * product_values[item][location]
                                for item in inv_shortfall
                                for location in inv_shortfall[item]
                                for period in inv_shortfall[item][location])
    prob.objective = inv_shortfall_objective
    solver = HiGHS()
    prob.solve(solver)
    optimal_value = round(prob.objective.value(),5)
    update_objective(conn, LpStatus[prob.status], "Inventory Shortfall Optimization", optimal_value)
    if LpStatus[prob.status] == 'Optimal':
        if optimal_value <= 0:
            prob += inv_shortfall_objective <= 0, 'inv_shortfall'
        else:
            prob += inv_shortfall_objective <= optimal_value + 1, 'inv_shortfall'

    prob.objective = prob_objective
    return prob, inv_shortfall

def minimize_demand_shortfall(conn, prob, shortfall_objective):
    prob_objective = prob.objective
    prob.objective = shortfall_objective
    solver = HiGHS()
    prob.solve(solver)
    optimal_val = round(prob.objective.value(),5)
    update_objective(conn, LpStatus[prob.status], "Total Demand Shortfall", optimal_val)
    if LpStatus[prob.status] == 'Optimal':
        if optimal_val <= 0:
            prob += shortfall_objective <= 0, f"dem_shortfall"
        else:
            prob += shortfall_objective <= optimal_val + 1, f"dem_shortfall"
    prob.objective = prob_objective
    return prob

def minimize_production_shortfall(conn, prob, production_dict):
    shortfall_dict = {}
    for item, location, period, min_prod, max_prod in conn.execute(get_production_sql):
        min_prod = float(min_prod)
        max_prod = float(max_prod)
        if min_prod > 0 and min_prod <= max_prod:
            var = LpVariable(name=f"prod_shortfall_{item}_{location}_{period}", lowBound=0, 
                upBound=min_prod, cat=LpContinuous)
            if item not in shortfall_dict:
                shortfall_dict[item] = {location: {period: var}}
            elif location not in shortfall_dict[item]:
                shortfall_dict[item][location] = {period: var}
            else:
                shortfall_dict[item][location][period] = var

            prob += production_dict[item][location][period] + var >= min_prod

    shortfall_var = list(shortfall_dict[item][location][period] 
                           for item in shortfall_dict
                           for location in shortfall_dict[item]
                           for period in shortfall_dict[item][location])
    if len(shortfall_var) == 0:
        return prob
    prob_objective = prob.objective
    prob.objective = lpSum(shortfall_var)
    solver = HiGHS()
    prob.solve(solver)
    update_objective(conn, LpStatus[prob.status], "Total Production Shortfall", 
                     round(prob.objective.value(),4))
    for item in shortfall_dict:
        for location in shortfall_dict[item]:
            for period in shortfall_dict[item][location]:
                var = shortfall_dict[item][location][period]
                var_val = var.varValue
                if var_val <= 0:
                    prob += var == 0
                else:
                    print(item, location, period, var_val)
                    prob += var <= var_val + 0.0001

    prob.objective = prob_objective
    return prob


def optimize_initial_inventory(conn, prob, initial_inv_objective):
    prob_objective = prob.objective
    prob.objective = initial_inv_objective
    prob.solve()
    update_objective(conn, LpStatus[prob.status], "Initial Inventory Optimization", 
                     round(prob.objective.value(),4))
    if LpStatus[prob.status] == 'Optimal':
        optimal_val = round(prob.objective.value(),5) + 1
        prob += initial_inv_objective <= optimal_val, f"initial_inv_shortfall"
    prob.objective = prob_objective
    return prob
