from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# CONNECTIONS
connection = create_db_connection("localhost", "linux", "", "kula_db")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection(
    "localhost", "linux", "", "kula_database_v2")


credit_read = "SELECT * FROM c_credits;"
credits_table_data = read_query(connection, credit_read)


# # INSERT CREDIT STATES FIRST

for credit in credits_table_data:
    transaction_id = credit[2]
    member_id = credit[4]
    created_by = credit[18]
    updated_by = credit[19]
    credit_total = credit[5]
    capital = credit[6]
    interests = credit[7]
    paid_capital = credit[8]
    interests_paid = credit[9]
    paid_credit = credit[10]
    balance = credit[11]
    description = credit[12]
    state = credit[13]
    created_at = credit[15]
    entity = credit[14]
    deleted_at = credit[17]
    updated_at = credit[16]
    reference = credit[3]

    if (state == "PENDING"):
        state = 1
    elif (state == "FAILED"):
        state = 2
    else:
        state = 3

    end_date = add_35_years_or_not(deleted_at, created_at, updated_at)

    if transaction_id is None:
        transaction_id = 'null'

    if updated_at is None and created_at is not None:
        updated_at = created_at

    if reference is None:
        reference = ''

    if member_id == 192 or member_id == 193 or member_id == 194 or member_id == 195:
        entity = 2  # KULA
    else:
        entity = 1

    date = f"SELECT cri_payment_date FROM cri_credit_installment WHERE cr_id = {credit[1]} LIMIT 1;"
    cri_payment_date = read_query(connection_kula, date)

    if len(cri_payment_date) >= 1:
        cri_payment_date = cri_payment_date[0][0]
        if updated_by is None:
            migrate_credit = f"INSERT INTO cr_credit (row_id, cr_id, t_id, c_id, cs_id, par_id, cr_total_amount, cr_capital, cr_paid_capital, cr_interests, cr_interests_paid, cr_paid_credit, cr_balance, cr_description, cr_credit_reference, cr_payment_date, cr_created_on, cr_modified_on, cr_start_date, cr_end_date, cr_created_by, cr_modified_by) VALUES ({credit[0]},{credit[1]},{transaction_id},{member_id}, {state}, {entity}, {credit_total}, {capital}, {paid_capital}, {interests}, {interests_paid}, {paid_credit}, {balance}, '{description}', '{reference}', '{cri_payment_date}', '{created_at}','{updated_at}','{created_at}','{end_date}',{created_by}, {created_by});"
        else:
            migrate_credit = f"INSERT INTO cr_credit (row_id, cr_id, t_id, c_id, cs_id, par_id, cr_total_amount, cr_capital, cr_paid_capital, cr_interests, cr_interests_paid, cr_paid_credit, cr_balance, cr_description, cr_credit_reference, cr_payment_date, cr_created_on, cr_modified_on, cr_start_date, cr_end_date, cr_created_by, cr_modified_by) VALUES ({credit[0]},{credit[1]},{transaction_id},{member_id}, {state}, {entity}, {credit_total}, {capital}, {paid_capital}, {interests}, {interests_paid}, {paid_credit}, {balance}, '{description}', '{reference}', '{cri_payment_date}', '{created_at}','{updated_at}','{created_at}','{end_date}',{created_by}, {updated_by});"
    else:
        if updated_by is None:
            migrate_credit = f"INSERT INTO cr_credit (row_id, cr_id, t_id, c_id, cs_id, par_id, cr_total_amount, cr_capital, cr_paid_capital, cr_interests, cr_interests_paid, cr_paid_credit, cr_balance, cr_description, cr_credit_reference, cr_created_on, cr_modified_on, cr_start_date, cr_end_date, cr_created_by, cr_modified_by) VALUES ({credit[0]},{credit[1]},{transaction_id},{member_id}, {state}, {entity}, {credit_total}, {capital}, {paid_capital}, {interests}, {interests_paid}, {paid_credit}, {balance}, '{description}', '{reference}', '{created_at}','{updated_at}','{created_at}','{end_date}',{created_by}, {created_by});"
        else:
            migrate_credit = f"INSERT INTO cr_credit (row_id, cr_id, t_id, c_id, cs_id, par_id, cr_total_amount, cr_capital, cr_paid_capital, cr_interests, cr_interests_paid, cr_paid_credit, cr_balance, cr_description, cr_credit_reference, cr_created_on, cr_modified_on, cr_start_date, cr_end_date, cr_created_by, cr_modified_by) VALUES ({credit[0]},{credit[1]},{transaction_id},{member_id}, {state}, {entity}, {credit_total}, {capital}, {paid_capital}, {interests}, {interests_paid}, {paid_credit}, {balance}, '{description}', '{reference}', '{created_at}','{updated_at}','{created_at}','{end_date}',{created_by}, {updated_by});"

    fk = "SET FOREIGN_KEY_CHECKS=0;"
    execute_query(connection_kula, fk)
    execute_query(connection_kula, migrate_credit)


# credit_read = "SELECT * FROM cp_credit_prestacaos;"
# prestacoes_table = read_query(connection, credit_read)

# for prest in prestacoes_table:
#     migrate_credit = f"UPDATE cr_credit SET cr_payment_date = '{prest[13]}' WHERE cr_id = {prest[2]};"
#     execute_query(connection_kula, migrate_credit)
