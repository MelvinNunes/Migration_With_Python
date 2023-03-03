from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM cri_credit_installment;"
data = read_query(connection_kula, read)

for cri in data:
    created_at = cri[13]
    updated_at = cri[14]
    start_date = cri[15]
    deleted_at = cri[16]

    if start_date is None:
        start_date = created_at

    if created_at is None:
        created_at = updated_at

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    migrate = f"UPDATE cri_credit_installment SET cri_created_on = '{created_at}' WHERE row_id = {cri[0]};"
    execute_query(connection_kula, migrate)
