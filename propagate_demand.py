import sys
from cl_compute.sql_connector import sql_connect
from cl_compute.utilityMethods.propogate_demand import main as propogate_demand


if sys.platform != 'emscripten':
    thisDB = r"C:\Users\akhil\Downloads\SCL.sqlite3"

with sql_connect(thisDB) as conn:
    conn.execute("DELETE FROM T_SolverLog")
    propogate_demand(conn)