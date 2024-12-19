import apsw

class sql_connect():
    def __init__(self, db_name: str):
        self.cursor = get_cursor(db_name)
        self.db_name = db_name

    def __enter__(self):
        self.cursor.execute("begin")
        return scc_cursor(self.cursor, self.db_name)

    def __exit__(self, exception_type, exception_value, traceback_val):
        if exception_type:
            try:
                self.cursor.execute("ROLLBACK")
            except:
                pass
            self.cursor.close()
        else:
            self.cursor.execute("COMMIT")
            self.cursor.close()


def get_cursor(db_name):
    connection = apsw.Connection(db_name)
    return connection.cursor()


class scc_cursor():
    def __init__(self, conn, db_name):
        self.conn = conn
        self.db_name = db_name

    def execute(self, query, args=tuple()):
        try:
            self.conn.execute(query, args)
        except Exception as ex:
            print(f"query: {query}")
            if len(args) > 0:
                print(f"arguments: {args}")
            raise ex
        return self.conn

    def intermediate_commit(self):
        try:
            self.conn.execute("COMMIT")
            self.conn.execute("BEGIN")
        except Exception as ex:
            raise Exception(f"Error occured {ex}")

def insert_log(conn, message_str):
    print(message_str)
    query = "insert into T_SolverLog (LogMessage) values (?)"
    conn.execute(query, (message_str,))
    conn.intermediate_commit()