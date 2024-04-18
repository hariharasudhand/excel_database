import pandas as pd
import sqlite3
import os
from tqdm import tqdm

def create_database_from_excel(excel_file, db_file):
    print("Load init")

    # Create a connection to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Read Excel file with multiple sheets using Pandas
    data_dict = pd.read_excel(excel_file, sheet_name=None)

    print("Preparing for table creation")

    # Create tables in the SQLite database from each sheet with a progress bar
    progress_bar = tqdm(data_dict.items(), desc="Creating tables", unit="table")
    for sheet_name, df in progress_bar:
        # Define table name based on sheet name and replace special characters
        table_name = sheet_name.replace(" ", "_").replace(":", "_").replace(";", "_")

        # Filter out columns with empty names or unnamed columns
        df_filtered = df.loc[:, ~df.columns.str.contains("^Unnamed")]

        # Check if the filtered DataFrame is empty
        if df_filtered.empty:
            print(f"Skipping empty sheet '{sheet_name}'")
            continue

        # Define the columns and data types based on the DataFrame
        columns = df_filtered.dtypes.to_dict()
        column_definitions = []
        for col_name, col_type in columns.items():
            # Sanitize column names
            sanitized_col_name = col_name.replace(" ", "_").replace(":", "_").replace(";", "_")

            # Determine data type for each column
            if col_type == 'object':
                col_type = 'TEXT'
            elif col_type == 'int64':
                col_type = 'INTEGER'
            elif col_type == 'float64':
                col_type = 'REAL'

            # Add column definition
            column_definitions.append(f"{sanitized_col_name} {col_type}")

        # Check if there are column definitions
        if column_definitions:
            # Create the table with the specified columns
            create_table_sql = f"CREATE TABLE {table_name} ({', '.join(column_definitions)});"

            # Print the SQL query for debugging
            print(f"Executing SQL: {create_table_sql}")

            # Execute the query
            cursor.execute(create_table_sql)
        else:
            print(f"No valid columns for table '{table_name}', skipping creation.")

        # Load the filtered DataFrame into the SQLite database
        # Check if df_filtered is empty (already checked earlier, but keeping this for completeness)
        if not df_filtered.empty:
            print(f"Inserting data into table {table_name}")
            df_filtered.to_sql(table_name, conn, if_exists='replace', index=False)
        else:
            print(f"No data to insert for table {table_name}")

    # Commit changes and close the connection
    conn.commit()
    conn.close()

def insert_data_from_excel(excel_file, db_file):
    # Create a connection to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Read Excel file with multiple sheets using Pandas
    data_dict = pd.read_excel(excel_file, sheet_name=None)

    # Insert data from each sheet into the corresponding table
    for sheet_name, df in data_dict.items():
        # Define table name based on sheet name
        table_name = sheet_name.replace(" ", "_")

        # Check if the table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        table_exists = cursor.fetchone() is not None
        
        # If the table exists, proceed with data insertion
        if table_exists:
            # Use tqdm to add a progress bar around the DataFrame iteration
            for index, row in tqdm(df.iterrows(), total=len(df), desc=f"Inserting into {table_name}"):
                # Define a unique condition based on the row data
                # Customize this condition based on your table's unique columns or primary key
                condition = ' AND '.join([f'"{col}" = \'{escape_special_chars(row[col])}\'' for col in df.columns])
               
                # Check if the row already exists in the table
                cursor.execute(f"SELECT 1 FROM {table_name} WHERE {condition};")
                row_exists = cursor.fetchone() is not None

                # If the row does not exist, insert it
                if not row_exists:
                    # Insert the row into the table
                    # Convert the row to a list of values
                    values = tuple(row)
                    # Create a placeholder for each value
                    placeholders = ', '.join(['?' for _ in row])
                    # Create the INSERT INTO SQL statement
                    columns = ', '.join([f'"{col}"' for col in df.columns])
                    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders});"
                    # Execute the INSERT statement with the values
                    cursor.execute(insert_sql, values)

    # Commit changes and close the database connection
    conn.commit()
    conn.close()

def run_query(db_file, query):

    print()
    print()
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Execute the query and fetch the results
    cursor.execute(query)
    results = cursor.fetchall()

    # Print the results
    for row in results:
        print("- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - ")
        print(row)

    # Close the database connection
    conn.close()

# Other functions and the main block of code remain unchanged
def escape_special_chars(value):
    # Check if the value is a string
    if isinstance(value, str):
        # Replace single quotes with two single quotes
        value = value.replace("'", "''")
        # You can add more replacements here if needed
    return value

# Replace the data file paths as per your setup
if __name__ == "__main__":
    excel_file_path = 'db/db.xlsx'  # Path to your Excel file
    db_file_path = excel_file_path + '_.db'  # Path to your SQLite database file
    if os.path.exists(db_file_path):
        print(f"File '{db_file_path}' exists. Ignoring DB Creation..")
    else:
        print(" db created in path ",db_file_path)
        # Create the SQLite database from the Excel file
        create_database_from_excel(excel_file_path, db_file_path)
       
    

    # Insert data into the SQLite database from the Excel file
    insert_data_from_excel(excel_file_path, db_file_path)

    print("Inserted data in tables")

    # Start a loop to listen for user input
    print("Enter SQL queries to execute on the database (type 'Q' to quit):")
    while True:
        query = input("### ")
        if query.upper() == "Q":
            print("Exiting...")
            break
        run_query(db_file_path, query)
