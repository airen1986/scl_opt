from ..sql_connector import insert_log

def main(conn):
    insert_log(conn, f"{'-'* 5} Model Validation Started {'-'* 5}")
    insert_log(conn, f"{'-'* 5} Model Validation Completed {'-'* 5}")