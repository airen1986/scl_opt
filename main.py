import sys
from cl_compute.sql_connector import sql_connect
from cl_compute.utilityMethods.clean_model import main as clean_model
from cl_compute.utilityMethods.validate_model import main as validate_model


if sys.platform != 'emscripten':
    thisDB = r"C:\Users\akhil\Downloads\SCL.sqlite3"

with sql_connect(thisDB) as conn:
    clean_model(conn)
    validate_model(conn)