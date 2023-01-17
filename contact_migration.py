from main_functions import create_db_connection, create_server_connection, read_query, execute_query


# CONNECTION TO OLD DATABASE
connection_kula = create_db_connection(
    "localhost", "root", "", "kula_old_data")


# CONNECTION TO NEW DATABASE
connection_six = create_db_connection(
    "localhost", "root", "", "six_database")

# READING DATA FROM OLD DATABASE
read = "SELECT * FROM ul_ussd_logs;"
data = read_query(connection_kula, read)

# READING DATA FROM NEW DATABASE
read_new = "SELECT * FROM su_session_ussd;"
data_new = read_query(connection_six, read_new)

for log in data:
    session_id = log[6]
    contact = log[3]
    for six in data_new:
        row_id = six[0]
        session_six = six[4]
        if session_id == session_six:
            migrate = f"UPDATE su_session_ussd SET contact = '{contact}' WHERE row_id={row_id};"
            execute_query(connection_six, migrate)
