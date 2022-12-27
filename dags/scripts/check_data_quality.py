from time import time,strftime
import ibm_db 

conn = None
def run_data_quality_check(**options):
    print("_" * 50)
    print(strftime("%H:%M, %d-%m-%Y"))
    
    testname = options.pop("testname")
    test = options.pop("test")
    
    print(f"Starting test {testname}")
    start_time = time()
    status = test(**options)
    end_time = time()
    
    print(f"Finished test {testname}")
    print("Duration : ", str(end_time - start_time))
    print(f"Test Passed {status}")
    options.pop("conn")
    
    print("Test Parameters:")
    for key,value in options.items():
        print(f"\t{key} = {value}")
    print()
    
    return testname,options.get('table'),options.get('column'),status


def check_for_nulls(column, table, conn=conn):
    SQL = f'SELECT COUNT({column}) FROM "{table}" WHERE {column} IS NULL'
    stmt = ibm_db.exec_immediate(conn, SQL)
    sql_res = ibm_db.fetch_tuple(stmt)
    test_res = 0
    while sql_res != False:
        # print(sql_res[0])
        test_res += sql_res[0]
        sql_res = ibm_db.fetch_tuple(stmt)
        
    # ibm_db.close(conn)
    return test_res == 0


def check_for_min_max(column, table, minimum, maximum, conn=conn):
    SQL = f'SELECT COUNT(*) FROM "{table}" where {column} < {minimum} or {column} > {maximum}'
    stmt = ibm_db.exec_immediate(conn, SQL)
    sql_res = ibm_db.fetch_tuple(stmt)
    test_res = 0
    while sql_res != False:
        test_res += sql_res[0]
        sql_res = ibm_db.fetch_tuple(stmt)
        
    return test_res == 0


def check_for_valid_values(column, table, valid_values=None, conn=conn):
    SQL = f'SELECT DISTINCT {column} FROM "{table}"'
    stmt = ibm_db.exec_immediate(conn, SQL)
    sql_res = ibm_db.fetch_tuple(stmt)
    actual_values = []
    while sql_res != False:
        if sql_res[0] == 'Unknown': 
            sql_res = ibm_db.fetch_tuple(stmt) 
            continue
        if len(sql_res[0]) == 0:
            sql_res = ibm_db.fetch_tuple(stmt) 
            continue
        actual_values.append(sql_res[0])
        sql_res = ibm_db.fetch_tuple(stmt)
        
    #print(result)
    print(actual_values)
    valid_values = [val.lower() for val in valid_values]
    status = [value.lower() in valid_values for value in actual_values]

    return all(status)


def check_for_duplicates(column, table, conn=conn):
    SQL = f'SELECT COUNT({column}) FROM "{table}" GROUP BY {column} HAVING COUNT({column}) > 1'
    stmt = ibm_db.exec_immediate(conn, SQL)
    sql_res = ibm_db.fetch_tuple(stmt)
    test_res = 0
    while sql_res != False:
        test_res += sql_res[0]
        sql_res = ibm_db.fetch_tuple(stmt)
    
    return test_res == 0