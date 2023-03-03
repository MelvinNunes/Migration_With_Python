from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM par_partner;"
data = read_query(connection_kula, read)

for partner in data:
    created_at = partner[8]
    updated_at = partner[9]
    start_date = partner[10]
    deleted_at = partner[11]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    if start_date is None:
        start_date = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    migrate = f"UPDATE par_partner SET par_end_date = '{end_date}' WHERE row_id = {partner[0]};"
    execute_query(connection_kula, migrate)
