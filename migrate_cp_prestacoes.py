from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# CONNECTIONS
connection = create_db_connection("localhost", "linux", "", "kula_db")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection(
    "localhost", "linux", "", "kula_database_v2")

credit_prestacao_read = "SELECT * FROM cp_credit_prestacaos;"
credit_prestacao_table_data = read_query(connection, credit_prestacao_read)

for cp in credit_prestacao_table_data:
    credit_id = cp[2]
    created_by = cp[3]
    updated_by = cp[4]
    total_amount = cp[5]
    capital = cp[6]
    juros = cp[7]
    paid_capital = cp[8]
    interest_paid = cp[9]
    amount_paid = cp[10]
    description = cp[11]
    balance = cp[12]
    payment_date = cp[13]
    state = cp[14]
    deleted_at = cp[15]
    created_at = cp[16]
    updated_at = cp[17]

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    if (state == "PENDING"):
        state = 1
    elif (state == "FAILED"):
        state = 2
    else:
        state = 3

    if updated_at is None and created_at is not None:
        updated_at = created_at

    if description is None:
        description = 'NULL'

    fk = "SET FOREIGN_KEY_CHECKS=0;"
    execute_query(connection_kula, fk)

    migrate_cp = f"INSERT INTO cri_credit_installment VALUES ({cp[0]},{cp[1]},{credit_id}, {state}, {total_amount}, {capital}, {juros}, {paid_capital}, {interest_paid}, {amount_paid}, {balance}, '{payment_date}', '{description}', '{created_at}', '{updated_at}', '{created_at}', '{end_date}', {created_by}, {updated_by});"
    if updated_by is None:
        migrate_cp = f"INSERT INTO cri_credit_installment VALUES ({cp[0]},{cp[1]},{credit_id}, {state}, {total_amount}, {capital}, {juros}, {paid_capital}, {interest_paid}, {amount_paid}, {balance}, '{payment_date}', '{description}', '{created_at}', '{updated_at}', '{created_at}', '{end_date}', {created_by}, {created_by});"
    if updated_by is None and created_by is None:
        migrate_cp = f"INSERT INTO cri_credit_installment VALUES ({cp[0]},{cp[1]},{credit_id}, {state}, {total_amount}, {capital}, {juros}, {paid_capital}, {interest_paid}, {amount_paid}, {balance}, '{payment_date}', '{description}', '{created_at}', '{updated_at}', '{created_at}', '{end_date}', null, null);"
    if created_at is None:
        migrate_cp = f"INSERT INTO cri_credit_installment VALUES ({cp[0]},{cp[1]},{credit_id}, {state}, {total_amount}, {capital}, {juros}, {paid_capital}, {interest_paid}, {amount_paid}, {balance}, '{payment_date}', '{description}', '{updated_at}', '{updated_at}', '{created_at}', '{end_date}', null, null);"
    if created_at is None and updated_at is None:
        migrate_cp = f"INSERT INTO cri_credit_installment VALUES ({cp[0]},{cp[1]},{credit_id}, {state}, {total_amount}, {capital}, {juros}, {paid_capital}, {interest_paid}, {amount_paid}, {balance}, '{payment_date}', '{description}', null, null, null, null, null, null);"
    execute_query(connection_kula, migrate_cp)
