from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# CONNECTIONS
connection = create_db_connection("localhost", "root", "", "kula_old_data")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")

# GETTING DATA
read = "SELECT * FROM l_level"
data = read_query(connection, read)

for level in data:
    position = level[2]
    name = level[3]
    description = level[4]
    created_by = level[5]
    l_type = level[6]
    updated_by = level[7]
    start_date = level[8]
    created_at = level[9]
    updated_at = level[10]
    deleted_at = level[11]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    if l_type is None:
        migrate = f"INSERT INTO l_level VALUES ({level[0]},{level[1]}, {position}, '{name}', '{description}', {created_by}, null, {updated_by}, '{start_date}', '{end_date}', '{created_at}', '{updated_at}');"
    else:
        migrate = f"INSERT INTO l_level VALUES ({level[0]},{level[1]}, {position}, '{name}', '{description}', {created_by}, {l_type}, {updated_by}, '{start_date}', '{end_date}', '{created_at}', '{updated_at}');"

    execute_query(connection_kula, migrate)
