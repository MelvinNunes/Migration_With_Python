from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM p_payment;"
data = read_query(connection_kula, read)

for payment in data:
    created_at = payment[10]
    updated_at = payment[11]
    start_date = payment[12]
    deleted_at = payment[13]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    if created_at is None:
        created_at = updated_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    migrate = f"UPDATE p_payment SET p_created_on = '{created_at}' WHERE row_id = {payment[0]};"
    execute_query(connection_kula, migrate)
