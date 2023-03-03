from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# CONNECTIONS
connection = create_db_connection("localhost", "linux", "", "kula_db")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection(
    "localhost", "linux", "", "kula_database_v2")

cp_credit_payments_read = "SELECT * FROM cp_credit_payments;"
cp_credit_payments_table_data = read_query(connection, cp_credit_payments_read)

for cp in cp_credit_payments_table_data:
    credit_id = cp[2]
    transaction_id = cp[3]
    payment_method_id = cp[4]
    paid_amount = cp[5]
    amount_topay = cp[6]
    created_by = cp[7]
    updated_by = cp[8]
    created_at = cp[9]
    updated_at = cp[10]
    deleted_at = cp[11]
    state = 0
    cri = 0

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    fk = "SET FOREIGN_KEY_CHECKS=0;"
    execute_query(connection_kula, fk)

    if updated_by is None:
        if created_by is not None:
            updated_by = created_by
            migrate_cp = f"INSERT INTO p_payment (row_id, p_id, cr_id, cs_id, cri_id, t_id, pm_id, pd_id, p_details, p_amount, p_created_on, p_modified_on, p_start_date, p_end_date, p_created_by, p_modified_by) VALUES ({cp[0]},{cp[1]},{credit_id},{state}, {cri}, {transaction_id}, {payment_method_id}, 0, null, {paid_amount}, '{created_at}','{updated_at}','{created_at}','{end_date}',{created_by}, {updated_by});"
        else:
            migrate_cp = f"INSERT INTO p_payment (row_id, p_id, cr_id, cs_id, cri_id, t_id, pm_id, pd_id, p_details, p_amount, p_created_on, p_modified_on, p_start_date, p_end_date, p_created_by, p_modified_by) VALUES ({cp[0]},{cp[1]},{credit_id},{state}, {cri}, {transaction_id}, {payment_method_id}, 0, null, {paid_amount}, '{created_at}','{updated_at}','{created_at}','{end_date}',null, null);"
        execute_query(connection_kula, migrate_cp)
    else:
        migrate_cp = f"INSERT INTO p_payment (row_id, p_id, cr_id, cs_id, cri_id, t_id, pm_id, pd_id, p_details, p_amount, p_created_on, p_modified_on, p_start_date, p_end_date, p_created_by, p_modified_by) VALUES ({cp[0]},{cp[1]},{credit_id},{state}, {cri}, {transaction_id}, {payment_method_id}, 0, null, {paid_amount}, '{created_at}','{updated_at}','{created_at}','{end_date}',{created_by}, {updated_by});"
        execute_query(connection_kula, migrate_cp)
