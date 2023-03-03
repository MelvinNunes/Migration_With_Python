from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# CONNECTIONS
connection = create_db_connection("localhost", "linux", "", "kula_db")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection(
    "localhost", "linux", "", "kula_database_v2")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM ul_ussd_logs;"
data = read_query(connection, read)

for log in data:
    ul_id = log[1]
    ul_type = log[2]
    ul_contact = log[3]
    ul_text_message = log[4]
    ul_step_data = log[5]
    ul_session_id = log[6]
    last_menu = log[7]
    ul_service_type = log[8]
    l_id = log[9]
    l_type = log[11]
    ul_start_date = log[13]
    created_at = log[14]
    updated_at = log[15]
    deleted_at = log[16]
    created_by = log[10]
    updated_by = log[12]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    fk = "SET FOREIGN_KEY_CHECKS=0;"
    execute_query(connection_kula, fk)

    if ul_service_type is None and l_type is not None:
        migrate = f'''INSERT INTO ul_ussd_logs (row_id, ul_id, l_id, ul_service_type, 
        ul_type, ul_contact, ul_text_message, ul_step_data, ul_session_id, 
        ul_last_menu, l_type, ul_created_at, ul_updated_at, ul_start_date, 
        ul_end_date, ul_created_by, ul_modified_by) VALUES ({log[0]}, {log[1]}, {l_id}, null, 
        {ul_type}, '{ul_contact}', '{ul_text_message}', '{ul_step_data}', '{ul_session_id}', '{last_menu}', {l_type}, '{created_at}', '{updated_at}', '{created_at}', 
        '{end_date}', {created_by}, {updated_by});'''

    elif l_type is None and ul_service_type is not None:
        migrate = f'''INSERT INTO ul_ussd_logs (row_id, ul_id, l_id, ul_service_type, 
        ul_type, ul_contact, ul_text_message, ul_step_data, ul_session_id, 
        ul_last_menu, l_type, ul_created_at, ul_updated_at, ul_start_date, 
        ul_end_date, ul_created_by, ul_modified_by) VALUES ({log[0]}, {log[1]}, {l_id}, {ul_service_type}, 
        {ul_type}, '{ul_contact}', '{ul_text_message}', '{ul_step_data}', '{ul_session_id}', '{last_menu}', null, '{created_at}', '{updated_at}', '{created_at}', 
        '{end_date}', {created_by}, {updated_by});'''
    elif l_type is None and ul_service_type is None:
        migrate = f'''INSERT INTO ul_ussd_logs (row_id, ul_id, l_id, ul_service_type, 
        ul_type, ul_contact, ul_text_message, ul_step_data, ul_session_id, 
        ul_last_menu, l_type, ul_created_at, ul_updated_at, ul_start_date, 
        ul_end_date, ul_created_by, ul_modified_by) VALUES ({log[0]}, {log[1]}, {l_id}, null, 
        {ul_type}, '{ul_contact}', '{ul_text_message}', '{ul_step_data}', '{ul_session_id}', '{last_menu}', null, '{created_at}', '{updated_at}', '{created_at}', 
        '{end_date}', {created_by}, {updated_by});'''
    else:
        migrate = f'''INSERT INTO ul_ussd_logs (row_id, ul_id, l_id, ul_service_type, 
        ul_type, ul_contact, ul_text_message, ul_step_data, ul_session_id, 
        ul_last_menu, l_type, ul_created_at, ul_updated_at, ul_start_date, 
        ul_end_date, ul_created_by, ul_modified_by) VALUES ({log[0]}, {log[1]}, {l_id}, {ul_service_type}, 
        {ul_type}, '{ul_contact}', '{ul_text_message}', '{ul_step_data}', '{ul_session_id}', '{last_menu}', {l_type}, '{created_at}', '{updated_at}', '{created_at}', 
        '{end_date}', {created_by}, {updated_by});'''
    # migrate = f"UPDATE ul_ussd_logs SET ul_end_date = '{end_date}' WHERE row_id = {log[0]};"
    execute_query(connection_kula, migrate)
