import os, apsw, json, io

current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
this_folder = os.path.abspath(current_dir)
code_folder = os.path.join(os.path.dirname(this_folder), "cl_compute")
this_db = os.path.join(this_folder, "SupplyChainModel.db")
json_file = os.path.join(this_folder, "model_schema.json")

if os.path.exists(this_db):
    os.remove(this_db)

sql_file = os.path.join(this_folder, "db_schema.sql")
model_schema = os.path.join(this_folder, "model_schema.json")

with open(sql_file, "r", encoding="utf-8") as sql_file:
    sql_script = sql_file.read()

conn = apsw.Connection(this_db)
cursor = conn.cursor() 
cursor.execute(sql_script)
cursor.execute("DELETE FROM S_ExecutionFiles")
cursor.execute("DELETE FROM sqlite_sequence WHERE name =  'S_ExecutionFiles'")

file_contents = []
for filename in os.listdir(os.path.dirname(code_folder)):
    if filename.endswith(".py") or filename.endswith(".txt"):
        with open(os.path.join(os.path.dirname(code_folder), filename), "r", encoding="utf-8") as f:
            file_content = f.read()
            file_contents.append((filename, filename, file_content))

for root, dirs, files in os.walk(code_folder):
    for file in files:
        if not file.endswith(".pyc"):
            relative_root = os.path.relpath(root, os.path.dirname(code_folder))
            file_path = os.path.join(relative_root, file)
            file_path = file_path.replace("\\", "/")
            with open(os.path.join(root, file), "r", encoding="utf-8") as f:
                file_content = f.read()
                file_contents.append((file, file_path, file_content))

insert_query = "INSERT INTO S_ExecutionFiles (FileName, FilePath, FileData) VALUES (?, ?, ?)"
cursor.executemany(insert_query, file_contents)
cursor.close()

output = io.StringIO()
shell = apsw.Shell(stdout=output, db=conn)
shell.process_command(".dump")
out_sql = output.getvalue()
out_sql = "BEGIN TRANSACTION;\n" + out_sql.split("BEGIN TRANSACTION;", 1)[1]

with open(json_file, "r", encoding="utf-8") as f:
    json_data = json.load(f)

for key in json_data:
    print(key)

json_data["SCL_Model"] = out_sql

with open(json_file, "w", encoding="utf-8") as f:
    json.dump(json_data, f, indent=4)

