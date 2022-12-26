import ibm_db

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

create_dim_category_table = '''
    CREATE TABLE IF NOT EXISTS DimCategory (
        category_id INT NOT NULL, 
        category VARCHAR(1000),
        PRIMARY KEY(category_id)
    );
'''

def main():
    
    ibm_conn = ibm_db.connect(dsn, "", "")

    ibm_db.exec_immediate(ibm_conn, """DROP TABLE IF EXISTS DimCategory;""")
    ibm_db.exec_immediate(ibm_conn, create_dim_category_table)

    ibm_db.close(ibm_conn)

if __name__ == '__main__':
    main()