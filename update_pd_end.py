from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM t_transaction;"
data = read_query(connection_kula, read)

for transact in data:
    created_at = transact[8]
    updated_at = transact[9]
    start_date = transact[10]
    deleted_at = transact[11]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    if start_date is None:
        start_date = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    migrate = f"UPDATE t_transaction SET t_start_date = '{start_date}' WHERE row_id = {transact[0]};"
    execute_query(connection_kula, migrate)
