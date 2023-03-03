from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection(
    "localhost", "linux", "", "kula_database_v2")


# READING DATA FROM OLD DATABASE
read = "SELECT * FROM moc_member_otp_codes;"
data = read_query(connection_kula, read)

for otp in data:
    deleted_at = otp[13]
    created_at = otp[11]
    updated_at = otp[12]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    migrate = f"UPDATE moc_member_otp_codes SET moc_start_date = '{created_at}' WHERE row_id = {otp[0]};"
    execute_query(connection_kula, migrate)

    if deleted_at is None:
        migrate = f"UPDATE moc_member_otp_codes SET moc_end_date = '{end_date}' WHERE row_id = {otp[0]};"
        execute_query(connection_kula, migrate)
    else:
        migrate = f"UPDATE moc_member_otp_codes SET moc_end_date = '{deleted_at}' WHERE row_id = {otp[0]};"
        execute_query(connection_kula, migrate)
