from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not


# CONNECTIONS
connection = create_db_connection("localhost", "linux", "", "kula_db")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection(
    "localhost", "linux", "", "kula_database_v2")

# GETTING DATA
transaction_read = "SELECT * FROM t_transactions;"
transaction_table_data = read_query(connection, transaction_read)

# # # CHECK TRANSACTION TYPE

for transaction in transaction_table_data:
    member_id = transaction[2]
    created_by = transaction[3]
    updated_by = transaction[4]
    credit = transaction[5]
    debit = transaction[6]
    total_amount = transaction[7]
    state = transaction[8]
    reason_code = transaction[9]
    entity = transaction[10]
    deleted_at = transaction[11]
    created_at = transaction[12]
    updated_at = transaction[13]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    if state == 'COMPLETED':
        state = 3
    elif state == 'PENDING':
        state = 1
    else:
        state = 2

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    fk = "SET FOREIGN_KEY_CHECKS=0;"
    execute_query(connection_kula, fk)

    if created_by is None and updated_by is None:
        migrate_transaction = f"INSERT INTO t_transaction (row_id, t_id, c_id, t_created_on, t_modified_on, t_start_date, t_end_date, t_amount, t_total_amount, t_reason_code, cs_id) VALUES ({transaction[0]},{transaction[1]},{member_id},'{created_at}','{updated_at}','{created_at}','{end_date}', {total_amount}, {total_amount}, '{reason_code}', {state});"
    else:
        migrate_transaction = f"INSERT INTO t_transaction (row_id, t_id, c_id, t_created_on, t_modified_on, t_start_date, t_end_date, t_amount, t_total_amount, t_created_by, t_modified_by, t_reason_code, cs_id) VALUES ({transaction[0]},{transaction[1]},{member_id},'{created_at}','{updated_at}','{created_at}','{end_date}', {total_amount}, {total_amount}, {created_by}, {updated_by}, '{reason_code}', {state});"

    execute_query(connection_kula, migrate_transaction)
