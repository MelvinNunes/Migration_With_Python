from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# CONNECTIONS
connection = create_db_connection("localhost", "root", "", "kula_old_data")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM ru_rules;"
data = read_query(connection_kula, read)

for rule in data:
    created_at = rule[8]
    updated_at = rule[9]
    deleted_at = rule[11]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    migrate = f"UPDATE ru_rules SET ru_end_date = '{end_date}' WHERE row_id = {rule[0]};"
    execute_query(connection_kula, migrate)
