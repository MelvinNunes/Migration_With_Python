from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection(
    "localhost", "linux", "", "kula_database_v2")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM l_logs;"
data = read_query(connection_kula, read)

for log in data:
    deleted_at = log[13]
    created_at = log[10]
    updated_at = log[11]
    start_date = log[12]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    migrate = f"UPDATE l_logs SET l_start_date = '{created_at}', l_end_date = '{end_date}' WHERE row_id = {log[0]};"
    execute_query(connection_kula, migrate)
