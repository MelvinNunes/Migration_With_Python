from main_functions import create_db_connection, create_server_connection, read_query, execute_query
from years_logic import add_years, add_35_years_or_not

# CONNECTIONS
connection = create_db_connection("localhost", "linux", "", "kula_db")

# NEW CONNECTION TO NEW DATABASE
connection_kula = create_db_connection(
    "localhost", "linux", "", "kula_database_v2")

lt_logs_transaction_read = "SELECT * FROM lt_logs_transaction;"
lt_logs_transaction_table_data = read_query(
    connection, lt_logs_transaction_read)

for log in lt_logs_transaction_table_data:
    row = log[0]
    lt_id = log[2]
    amount = log[1]
    lt_type = log[3]
    msisdn = log[4]
    member_id = log[5]
    state = log[6]
    request = log[7]
    created_at = log[8]
    response = log[9]
    input_TransactionReference = log[10]
    input_CustomerMSISDN = log[11]
    input_amount = log[12]
    input_ThirdPartyReference = log[13]
    ServiceProviderCode = log[14]
    ResponseCode = log[15]
    ResponseDesc = log[16]
    TransactionID = log[17]
    ConversationID = log[18]
    output_ThirdPartyReference = log[19]
    created_by = log[20]
    transaction_id = log[21]
    modified_by = log[22]
    start_date = log[23]
    updated_at = log[24]
    end_date = log[25]

    if updated_at is None and created_at is not None:
        updated_at = created_at

    end_date = add_35_years_or_not(end_date, created_at, updated_at)

    fk = "SET FOREIGN_KEY_CHECKS=0;"
    execute_query(connection_kula, fk)

    migrate_logs = f'''INSERT INTO pd_payment_details (row_id, pd_id, pd_details, pd_created_on, pd_modified_on, 
    pd_start_date, pd_end_date, pd_formated_amount, pd_created_by, pd_modified_by, pd_input_TransactionReference, 
    pd_input_CustomerMSISDN, pd_payment_date, pd_amount, pd_input_Amount, pd_input_ThirdPartyReference, pd_input_ServiceProviderCode, 
    pd_output_ResponseCode, pd_output_ResponseDesc, pd_output_TransactionID, 
    pd_output_ThirdPartyReference, pd_output_ConversationID, pd_account_number) VALUES ({row},{lt_id},'{request}','{created_at}','{updated_at}',
    '{created_at}','{end_date}','{amount}.00',{created_by}, {modified_by}, '{input_TransactionReference}',
    '{input_CustomerMSISDN}', '{created_at}', {amount}, '{amount}', '{input_ThirdPartyReference}', '{ServiceProviderCode}'
    ,'{ResponseCode}','{ResponseDesc}','{TransactionID}','{output_ThirdPartyReference}',
    '{ConversationID}', '{input_CustomerMSISDN}');'''

    execute_query(connection_kula, migrate_logs)
