# import pandas as pd
# import json
# from datetime import datetime
# from Utilities.Source_Target_DB_Conn import MySQL_DB_Conn, Oracle_DB_Conn
# from Utilities.logging import Logs
#
# source_db_conn = MySQL_DB_Conn()
# target_db_conn = Oracle_DB_Conn()
#
# # Calling Log_gen function from Utilities.
# dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
# logger = Logs.Log_Gen(f'C:\\NSR_Python_Projects\\Python_Project_Version2\\ETL_Testing_Framework(Config Driven Approach)\\Logs\\Table_existence_{dt}.log')
#
# with open(r'C:\NSR_Python_Projects\Python_Project_Version2\ETL_Testing_Framework(Config Driven Approach)\Config\SQL_Queries_config\Tables_Checking.json','r') as Tables_chk_file:
#      table_list = json.load(Tables_chk_file)
#
# with open(r'C:\NSR_Python_Projects\Python_Project_Version2\ETL_Testing_Framework(Config Driven Approach)\Config\SQL_Queries_config\Tables_Existance_Validations.json','r') as Data_Validation_file:
#     validations = json.load(Data_Validation_file)
#
# def Validate_Source_table_exist(db_conn):
#
#     db_conn = source_db_conn
#     SQL_query = validations['Table existence check']['Source_Query']
#     tables_chk = table_list.get('Source_tables',[])
#     results_list = []
#     for table in tables_chk:
#         query = SQL_query.format(table_name = table)
#
#         try:
#             result = pd.read_sql(query,db_conn)
#             if result.empty :
#                pass
#             else:
#                 status_result ="table  exists"
#         except Exception :
#              status_result = "Table does not exists"
#
#         results_list.append({"Table Name":table,"Status":status_result})
#     source_dataframe = pd.DataFrame(results_list)
#     return source_dataframe
#
# #def Validate_target_table_exist(db_conn,SQL_query,tables_list):
# def Validate_target_table_exist(db_conn):
#     db_conn = target_db_conn
#     SQL_query = validations["Table existence check"]["Target_Query"]
#     tables_list = table_list.get('Target_tables',[])
#     results_list = []
#     for table in tables_list:
#         query = SQL_query.format(table_name=table)
#
#         try:
#             result = pd.read_sql(query,db_conn)
#             if result.empty or result.iloc[0,0]<=0:
#                 pass
#             else:
#                 status_result ="table exists"
#         except Exception :
#             status_result = "Table does not exists"
#         results_list.append({"Table Name":table,"Status":status_result})
#     target_dataframe = pd.DataFrame(results_list)
#     return target_dataframe
#
# #Writing Output Result to excel file.
# dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
# output_path = f"C:\\NSR_Python_Projects\\Python_Project_Version2\\ETL_Testing_Framework(Config Driven Approach)\\Output_Result\\Table_existence_validation_{dt}.xlsx"
# with pd.ExcelWriter(output_path,engine='xlsxwriter') as writer:
#     #Validate_Source_table_exist(source_db_conn,None,None).to_excel(writer,sheet_name="Source Table Existence",index=False)
#     #Validate_target_table_exist(target_db_conn,None,None).to_excel(writer,sheet_name="Target Table Existence",index=False)
#     Validate_Source_table_exist(source_db_conn).to_excel(writer,sheet_name="Source Table Existence",index=False)
#     Validate_target_table_exist(target_db_conn).to_excel(writer,sheet_name="Target Table Existence",index=False)
#
# logger.info(f"Table existence validation results have been written to {output_path}")









#modified  code

import pandas as pd
import json
from datetime import datetime
from Test_Cases import MySQL_DB_Conn, Oracle_DB_Conn
from Test_Cases import Logs

source_db_conn = MySQL_DB_Conn()
target_db_conn = Oracle_DB_Conn()

# Calling Log_gen function from Utilities.
dt = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
logger = Logs.Log_Gen(
    f'C:\\NSR_Python_Projects\\Python_Project_Version2\\ETL_Testing_Framework(Config Driven Approach)\\Logs\\Table_existence_{dt}.log')

with open(
        r'C:\NSR_Python_Projects\Python_Project_Version2\ETL_Testing_Framework(Config Driven Approach)\Config\SQL_Queries_config\Tables_Checking.json',
        'r') as Tables_chk_file:
    table_list = json.load(Tables_chk_file)

with open(
        r'C:\NSR_Python_Projects\Python_Project_Version2\ETL_Testing_Framework(Config Driven Approach)\Config\SQL_Queries_config\Tables_Existance_Validations.json',
        'r') as Data_Validation_file:
    validations = json.load(Data_Validation_file)


def check_source_conn():
    try:
        test_query = "SELECT 1 FROM dual"
        source_results = pd.read_sql(test_query, source_db_conn)
        if source_results.iloc[0, 0] == 1:
            status = "Successful"
        else:
            status = "Unsuccessful"
    except Exception as e:
        logger.error(f"Source DB Connection failed: {e}")
        status = "Unsuccessful"

    Source_conn_df = pd.DataFrame([{"Database": "MySQL", "Status": status}])
    return status, Source_conn_df


def check_target_conn():
    try:
        test_query = "SELECT 1 FROM dual"
        target_results = pd.read_sql(test_query, target_db_conn)
        if target_results.iloc[0, 0] == 1:
            status = "Successful"
        else:
            status = "Unsuccessful"
    except Exception as e:
        logger.error(f"Target DB Connection failed: {e}")
        status = "Unsuccessful"

    target_conn_df = pd.DataFrame([{"Database": "Oracle", "Status": status}])
    return status, target_conn_df


def Validate_Source_table_exist(db_conn):
    SQL_query = validations['Table existence check']['Source_Query']
    tables_chk = table_list.get('Source_tables', [])
    results_list = []
    for table in tables_chk:
        query = SQL_query.format(table_name=table)
        try:
            result = pd.read_sql(query, db_conn)
            if result.empty:
                status_result = "Table does not exist"
            else:
                status_result = "Table exists"
        except Exception:
            status_result = "Table does not exist"
        results_list.append({"Table Name": table, "Status": status_result})
    return pd.DataFrame(results_list)


def Validate_target_table_exist(db_conn):
    SQL_query = validations["Table existence check"]["Target_Query"]
    tables_list = table_list.get('Target_tables', [])
    results_list = []
    for table in tables_list:
        query = SQL_query.format(table_name=table)
        try:
            result = pd.read_sql(query, db_conn)
            if result.empty or result.iloc[0, 0] <= 0:
                status_result = "Table does not exist"
            else:
                status_result = "Table exists"
        except Exception:
            status_result = "Table does not exist"
        results_list.append({"Table Name": table, "Status": status_result})
    return pd.DataFrame(results_list)


# Checking connections
source_status, source_conn_df = check_source_conn()
target_status, target_conn_df = check_target_conn()

# Define output path
output_path = f"C:\\NSR_Python_Projects\\Python_Project_Version2\\ETL_Testing_Framework(Config Driven Approach)\\Output_Result\\Table_existence_validation_{dt}.xlsx"

# Writing results to Excel file
with pd.ExcelWriter(output_path, engine='xlsxwriter') as writer:
    source_conn_df.to_excel(writer, sheet_name="Source_Db_Conn_chk", index=False)
    target_conn_df.to_excel(writer, sheet_name="Target_Db_Conn_chk", index=False)

    # Write table validation results only if both connections are successful
    if source_status == "Successful" and target_status == "Successful":
        Validate_Source_table_exist(source_db_conn).to_excel(writer, sheet_name="Source Table Existence", index=False)
        Validate_target_table_exist(target_db_conn).to_excel(writer, sheet_name="Target Table Existence", index=False)

logger.info(f"Table existence validation results have been written to {output_path}")
