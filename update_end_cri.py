from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM cri_credit_installment;"
data = read_query(connection_kula, read)

for cri in data:
    deleted_at = cri[17]
    created_at = cri[14]
    updated_at = cri[15]
    start_date = cri[16]
    state = cri[4]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    if (state == "PENDING"):
        state = 1
    elif (state == "FAILED"):
        state = 2
    else:
        state = 3

    migrate = f"UPDATE cri_credit_installment SET cs_id = {state} WHERE row_id = {cri[0]};"
    execute_query(connection_kula, migrate)
