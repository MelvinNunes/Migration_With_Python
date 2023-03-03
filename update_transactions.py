from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM t_transaction;"
data = read_query(connection_kula, read)

for transaction in data:
    state = transaction[7]

    if (state == "PENDING"):
        state = 1
    elif (state == "FAILED"):
        state = 2
    else:
        state = 3

    migrate = f"UPDATE t_transaction SET cs_id = '{state}' WHERE row_id = {transaction[0]};"
    execute_query(connection_kula, migrate)
