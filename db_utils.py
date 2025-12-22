from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

SCHEMA_FILE = "schema.sql"
HOST_ID = "localhost"
USER_NAME = "root"
USER_PASSWORD = "root"

DB_NAME = "traffic_db"

def get_engine(bound=""):
    """return enigne bound to server if bound is empty string else the database provided"""

    server_url = f"mysql+mysqlconnector://{USER_NAME}:{USER_PASSWORD}@{HOST_ID}/{bound}"

    try:
        server_engine = create_engine(server_url)
    except SQLAlchemyError as e:
        print(f"Error occured during engine creation: {e}")
    else:
        return server_engine

def apply_schema(engine):
    """checks for existing schema else apply schema and return engine bound to the database"""
    inspector = inspect(engine)
    db_names = inspector.get_schema_names()

    bool_apply_schema = False

    if DB_NAME not in db_names:
        print(f"{DB_NAME} not in server")
        try:
            print("opening schema file")
            with open(SCHEMA_FILE, "r", encoding="utf-8") as f:
                print("reading schema file")
                sql_content = f.read()
        except FileNotFoundError:
            print(f"ERROR: Schema file '{SCHEMA_FILE}' not found.")

        else:
            statements = [stmt.strip() for stmt in sql_content.split(";") if stmt.strip()]

            with engine.connect() as conn:
                try:
                    for stmt in statements:
                            print(f"executing schema command: {stmt}")
                            conn.execute(text(stmt))
                except SQLAlchemyError as e:
                    print(f"Error executing statement:\n{stmt}\n{e}")
                else:
                    print("schema successfully applied without any error")
                    bool_apply_schema = True
    else:
        bool_apply_schema = True
        print(f"{DB_NAME} already exists")
    
    if not bool_apply_schema: return

    engine = get_engine(bound=f"{DB_NAME}")
    inspector = inspect(engine)

    table_name = inspector.get_table_names()[0]
    print(f"Describe table {table_name}")
    columns = inspector.get_columns(table_name)
    pad = 30
    print(f"{'Name':<{pad}} {'Type':<{pad}} {'Nullable':<{pad}}")
    for col in columns:
        print(f"{col['name'] :<{pad}} {str(col['type']) :<{pad}} {col['nullable']}")

    print(f"returning server engine bound to {DB_NAME}")
    return engine
