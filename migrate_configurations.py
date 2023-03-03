from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# CONNECTIONS
connection = create_db_connection("localhost", "root", "", "kula_old_data")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")

# GETTING DATA
c_configuration_read = "SELECT * FROM c_configuration"
c_configuration_table_data = read_query(connection_kula, c_configuration_read)


for config in c_configuration_table_data:

    created_at = config[6]
    updated_at = config[7]
    deleted_at = config[9]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    migrate = f"UPDATE c_configuration SET c_end_date = '{end_date}' WHERE row_id = {config[0]};"
    execute_query(connection_kula, migrate)
