from db_connection  import get_db_connection

def extract_new_data():
    connection = get_db_connection()
    print("Connected successfully to PostgreSQL!")

    query =  "select order_id , order_date , order_customer_id , order_status from orders ;"
    cursor = connection.cursor()
    cursor.execute(query=query)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    print("Successfully fetched data from PostgreSQL!")
    cursor.close()
    connection.close()
    print("PostgreSQL connection closed successfully!")
    print(f"New Rows Extracted : {len(rows)}")
    return rows, columns






