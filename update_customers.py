from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM c_customer;"
data = read_query(connection_kula, read)

for member in data:
    modified_at = member[9]

    migrate = f"UPDATE c_customer SET c_created_on = '{modified_at}' WHERE row_id = {member[0]};"
    execute_query(connection_kula, migrate)
