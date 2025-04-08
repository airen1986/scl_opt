from ..sql_connector import insert_log
from ..utilityMethods.populate_periods import main as populate_periods
from .load_variables import *
from .load_constraints import *
from .import_output import main as import_sol
from pulp import LpProblem, LpMinimize, LpStatus, HiGHS

def main(conn, initialize_inv = False):
    insert_log(conn, f"{'-'* 5} LP Initialization Starts {'-'* 5}")
    populate_periods(conn)
    periods, all_combinations = get_master_data(conn)
    if len(all_combinations) == 0:
        insert_log(conn, f"No data for optimization")
        return
    if len(periods) == 0:
        insert_log(conn, f"No periods for optimization")
        return
    prob = LpProblem(f"The_Supply_Planning_Problem", LpMinimize)
    conn.execute("DELETE FROM O_Objective")
    initial_inv, initial_inv_objective = get_initial_inventory(conn, initialize_inv, periods[0])
    demand_dict = generate_demand_var(conn)
    inventory_dict,  inv_objective, product_values, holding_cost = generate_inventory_var(conn)
    demand_shortfall, demand_shortfall_objective = get_demand_shortfall_var(prob, demand_dict, 
                                                                            periods, product_values)
    prob, flow_dict, tpt_objective, inbound_flow = generate_flow_var(prob, conn, periods, holding_cost)
    production_dict = get_production_var(conn)
    prob, process_dict, prod_objective = get_process_var(prob, conn, production_dict)
    prob, bom_var = get_bom_var(prob, conn, process_dict)
    resource_dict = get_resource_var(conn)
    prob = add_resource_constraint(prob, conn, process_dict, resource_dict)
    prob, reg_demand_dict = get_reg_cal_demand(prob, conn, demand_dict)
    prob, expiry_var, expiry_holding_cost = get_expiry_variable(conn, prob, periods, flow_dict, 
                                                                demand_dict, bom_var, holding_cost)
    prob = demand_matching(prob, periods, inventory_dict, production_dict, initial_inv, flow_dict, 
                           demand_dict, all_combinations, bom_var, reg_demand_dict, inbound_flow, 
                           expiry_var)
    prob = add_min_relase_time_constraint(conn, prob, periods, inventory_dict, flow_dict, demand_dict, 
                                          bom_var, initial_inv, reg_demand_dict)
    insert_log(conn, f"{'-'* 5} LP Initialization Completed {'-'* 5}")
    prob = minimize_production_shortfall(conn, prob, production_dict)
    prob = minimize_demand_shortfall(conn, prob, demand_shortfall_objective)
    if initialize_inv:
        prob = optimize_initial_inventory(conn, prob, initial_inv_objective)
    prob, inv_shortfall = minimize_inventory_shortfall(prob, conn, inventory_dict, flow_dict, periods, 
                                            demand_dict, bom_var, product_values, reg_demand_dict)
    prob.objective = tpt_objective + inv_objective + prod_objective + expiry_holding_cost
    solver = HiGHS()
    prob.solve(solver)
    update_objective(conn, LpStatus[prob.status], "Total Cost Optimization", round(prob.objective.value(),4))
    conn.intermediate_commit()
    import_sol(conn, demand_dict, inventory_dict, reg_demand_dict, process_dict, 
               flow_dict, inbound_flow, demand_shortfall, inv_shortfall, all_combinations, periods, 
               initial_inv, bom_var, expiry_var)
