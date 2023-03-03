from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# CONNECTIONS
connection = create_db_connection("localhost", "root", "", "kula_old_data")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection("localhost", "root", "", "kula_new")


# READING DATA FROM OLD DATABASE
users_read = "SELECT * FROM c_customer;"
users_table_data = read_query(connection_kula, users_read)

# MIGRATED USER TABLE

for user in users_table_data:
    alternative_contact = user[11]
    contact = user[10]
    deleted_at = user[16]
    created_at = user[13]
    updated_at = user[14]
    document_id = user[7]
    created_by = user[17]
    updated_by = user[18]
    last_name = user[5]

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)
    # INSERTING THE DATA INTO NEW DB

    if updated_at is None and created_at is not None:
        updated_at = created_at

    # migrate_user = f"UPDATE c_customer SET c_end_date = '{end_date}' WHERE row_id = {user[0]};"
    if created_at is None:
        created_at = updated_at

    if created_by is None:
        created_by = updated_by

    if contact is not None:
        migrate_contact = f"INSERT INTO cpn_customer_phone_numbers VALUES ({user[0]}, {user[1]}, {user[1]}, '{contact}', '', '{created_at}','{updated_at}', '{created_at}', '{end_date}', {created_by}, {updated_by});"
        execute_query(connection_kula, migrate_contact)

    if alternative_contact is not None:
        migrate_contact = f"INSERT INTO cpn_customer_phone_numbers VALUES ({user[0]}, {user[1]}, {user[1]}, '{alternative_contact}', '', '{created_at}','{updated_at}', '{created_at}', '{end_date}', {created_by}, {updated_by});"
        execute_query(connection_kula, migrate_contact)
