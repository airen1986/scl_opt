import sys
from cl_compute.sql_connector import sql_connect
from cl_compute.optimization.main import main as optimize


if sys.platform != 'emscripten':
    thisDB = r"C:\Users\akhil\Downloads\SCL.sqlite3"

with sql_connect(thisDB) as conn:
    conn.execute("DELETE FROM T_SolverLog")
    optimize(conn)