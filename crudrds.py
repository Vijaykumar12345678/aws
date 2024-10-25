"""
@Author:Vijay Kumar M N
@Date: 2024-10-25
@Last Modified by:Vijay Kumar M N
@Last Modified: 2024-10-25
@Title :Python Program to interact with rds and crud operations
"""
import pymssql

# Function to connect to the database with error handling
def connect_to_database(endpoint, port, username, password, database):
    try:
        server = f"{endpoint}:{port}"
        conn = pymssql.connect(server=server, user=username, password=password, database=database,port=port)
        return conn
    except pymssql.OperationalError as e:
        print(f"Error connecting to the database: {e}")
        return None

# Function to check if the user has CREATE TABLE permission
def check_permissions(cursor):
    cursor.execute("SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE')")
    has_permission = cursor.fetchone()[0]
    if not has_permission:
        print("You do not have permission to create tables in this database.")
        return False
    return True

# Function to create table
def create_table(cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Employees' AND xtype='U')
        CREATE TABLE Employees (
            ID INT PRIMARY KEY,
            Name NVARCHAR(50),
            Age INT
        )
    """)
    print("Table 'Employees' created or already exists.")

# Function to insert data into the table
def insert_data(cursor):
    cursor.execute("INSERT INTO Employees (ID, Name, Age) VALUES (4, 'vijay', 23)")
    cursor.execute("INSERT INTO Employees (ID, Name, Age) VALUES (5, 'kumar', 25)")
    print("Data inserted into the table.")

# Function to read and display data
def read_data(cursor):
    cursor.execute("SELECT * FROM Employees")
    rows = cursor.fetchall()
    if rows:
        print("Current Employees in the database:")
        for row in rows:
            print("ID: {}, Name: {}, Age: {}".format(row[0], row[1], row[2]))
    else:
        print("No data found in the table.")

# Function to update data in the table
def update_data(cursor):
    cursor.execute("UPDATE Employees SET Age = 31 WHERE ID = 1")
    print("Data updated for employee with ID = 1.")

# Function to delete data from the table
def delete_data(cursor):
    cursor.execute("DELETE FROM Employees WHERE ID = 2")
    print("Data deleted for employee with ID = 2.")

# Function to handle CRUD operations based on user choice
def crud_operations(conn):
    cursor = conn.cursor()

    while True:
        print("\nOptions:")
        print("1. Create Table")
        print("2. Insert Data")
        print("3. Read Data")
        print("4. Update Data")
        print("5. Delete Data")
        print("6. Exit")
        
        choice = input("Select an option: ")

        if choice == '1':
            if check_permissions(cursor):
                create_table(cursor)
        elif choice == '2':
            insert_data(cursor)
            conn.commit()
        elif choice == '3':
            read_data(cursor)
        elif choice == '4':
            update_data(cursor)
            conn.commit()
        elif choice == '5':
            delete_data(cursor)
            conn.commit()
        elif choice == '6':
            break
        else:
            print("Invalid choice. Please try again.")

    cursor.close()

if __name__ == "__main__":
    # Example values (replace with your actual values)
    endpoint = 'database-1.c3e2sig42eps.ap-south-1.rds.amazonaws.com'  # RDS endpoint address
    port = 1433  # Default SQL Server port
    username = 'admin'  # Your RDS username
    password = 'vijaykumar123'  # Your RDS password

    # Prompt for the database name
    database = input("Enter your database name (not 'rdsadmin'): ")

    # Connect to the database
    conn = connect_to_database(endpoint, port, username, password, database)

    if conn:
        try:
            print("Connected to the database successfully.")
            crud_operations(conn)
        finally:
            conn.close()
            print("Connection closed.")
    else:
        print("Failed to connect to the database.")
