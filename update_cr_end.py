from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM cr_credit;"
data = read_query(connection_kula, read)

for credit in data:
    deleted_at = credit[18]
    created_at = credit[15]
    updated_at = credit[16]
    start_date = credit[17]

    state = credit[5]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    if start_date is None:
        start_date = created_at

    migrate = f"UPDATE cr_credit SET cr_start_date = '{start_date}' WHERE row_id = {credit[0]};"
    execute_query(connection_kula, migrate)
