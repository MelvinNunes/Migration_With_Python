from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# CONNECTIONS
connection = create_db_connection("localhost", "linux", "", "kula_db")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection(
    "localhost", "linux", "", "kula_database_v2")

# GETTING DATA
m_members_read = "SELECT * FROM m_members;"
m_members_table_data = read_query(connection, m_members_read)

# # MIGRATED TABLE (CHECK CONSTRAINT)

for member in m_members_table_data:
    null = 'NULL'

    u_id = member[2]
    created_by = member[3]
    updated_by = member[4]
    name = member[5]
    member_code = member[6]
    last_name = member[7]
    document_id = member[8]
    document_type = member[9]
    gender = member[10]
    contact = member[11]
    alternative_contact = member[12]
    birth_date = member[13]
    entity = member[14]
    deleted_at = member[15]
    created_at = member[16]
    updated_at = member[17]
    middle_name = null

    if member_code is None:
        member_code = null

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    if created_at is None and deleted_at is None:
        migrate_member = f"INSERT INTO c_customer (row_id, c_id, c_unique_code, c_first_name, c_middle_name, c_last_name, c_email, c_created_by, c_modified_by) VALUES ({member[0]},{member[1]}, '{member_code}', '{name}', '{middle_name}', '{last_name}', 'NULL', {created_by}, {updated_by});"
    else:
        migrate_member = f"INSERT INTO c_customer (row_id, c_id, c_unique_code, c_first_name, c_middle_name, c_last_name, c_email, c_created_on, c_modified_on, c_start_date, c_end_date, c_created_by, c_modified_by) VALUES ({member[0]},{member[1]}, '{member_code}', '{name}', '{middle_name}', '{last_name}', 'NULL', '{created_at}', '{updated_at}', '{created_at}', '{end_date}', {created_by}, {updated_by});"

    migrate_contact = f"INSERT INTO cpn_customer_phone_numbers VALUES ({member[0]},{member[1]}, {member[1]}, '{contact}', '', '{created_at}','{updated_at}', '{created_at}', '{end_date}', {created_by}, {updated_by});"

    if created_at is None and deleted_at is None:
        migrate_contact = f"INSERT INTO cpn_customer_phone_numbers (row_id, cpn_id, c_id, cpn_phone_number, cpn_description, cpn_created_by, cpn_modified_by) VALUES ({member[0]},{member[1]}, {member[1]}, '{contact}', '', {created_by}, {updated_by});"

    if alternative_contact is not None:
        migrate_contact = f"INSERT INTO cpn_customer_phone_numbers VALUES ({member[0]},{member[1]}, {member[1]}, '{alternative_contact}', '', '{created_at}','{updated_at}', '{created_at}', '{end_date}', {created_by}, {updated_by});"

    if alternative_contact is not None and created_at is None and deleted_at is None:
        migrate_contact = f"INSERT INTO cpn_customer_phone_numbers (row_id, cpn_id, c_id, cpn_phone_number, cpn_description, cpn_created_by, cpn_modified_by) VALUES ({member[0]},{member[1]}, {member[1]}, '{alternative_contact}', '', {created_by}, {updated_by});"

    migrate_document = f"INSERT INTO cdo_customer_document (row_id, cdo_id, c_id, cdo_created_on, cdo_modified_on, cdo_start_date, cdo_end_date) VALUES ({member[0]},{member[1]},{member[1]},'{created_at}','{updated_at}','{created_at}','{end_date}');"

    if created_at is None and deleted_at is None:
        migrate_document = f"INSERT INTO cdo_customer_document (row_id, cdo_id, c_id) VALUES ({member[0]},{member[1]},{member[1]});"

    execute_query(connection_kula, migrate_member)
    execute_query(connection_kula, migrate_contact)
    execute_query(connection_kula, migrate_document)
