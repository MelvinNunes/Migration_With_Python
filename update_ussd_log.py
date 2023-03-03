from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM ul_ussd_logs;"
data = read_query(connection_kula, read)

for usssd in data:
    created_at = usssd[11]
    updated_at = usssd[12]
    start_date = usssd[13]
    deleted_at = usssd[14]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    if start_date is None:
        start_date = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    migrate = f"UPDATE ul_ussd_logs SET ul_end_date = '{end_date}' WHERE row_id = {usssd[0]};"
    execute_query(connection_kula, migrate)
