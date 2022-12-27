import pandas as pd
import ibm_db
from tabulate import tabulate

from scripts import mytests

from scripts.check_data_quality import check_for_nulls
from scripts.check_data_quality import check_for_min_max
from scripts.check_data_quality import check_for_valid_values
from scripts.check_data_quality import check_for_duplicates
from scripts.check_data_quality import run_data_quality_check

# Connect to DB2
dsn_hostname = "2f3279a5-73d1-4859-88f0-a6c3e6b4b907.c3n41cmd0nqnrk39u98g.databases.appdomain.cloud"
dsn_uid = "prg79418"        
dsn_pwd = "QOKWifsXGuPcg5Li"      
dsn_port = "30756"                
dsn_database = "bludb"            
dsn_driver = "{IBM DB2 ODBC DRIVER}"       
dsn_protocol = "TCPIP"           
dsn_security = "SSL"              

dsn = (
    "DRIVER={0};"
    "DATABASE={1};"
    "HOSTNAME={2};"
    "PORT={3};"
    "PROTOCOL={4};"
    "UID={5};"
    "PWD={6};"
    "SECURITY={7};"
).format(dsn_driver, dsn_database, dsn_hostname, dsn_port, dsn_protocol, dsn_uid, dsn_pwd, dsn_security)

def main():
    ibm_conn = ibm_db.connect(dsn, "", "")

    print("Connected to data warehouse")

    #Start of data quality checks
    results = []
    tests = {key:value for key,value in mytests.__dict__.items() if key.startswith('test')}
    for testname, test in tests.items():
        test['conn'] = ibm_conn
        results.append(run_data_quality_check(**test))

    #print(results)
    df = pd.DataFrame(results)
    df.index += 1
    df.columns = ['Test Name', 'Table', 'Column', 'Test Passed']
    print(tabulate(df, headers='keys', tablefmt='psql'))

    #End of data quality checks
    ibm_db.close(ibm_conn)
    print("Disconnected from data warehouse")

if __name__ == '__main__':
    main()