def get_connection():
    conn_str = (
       "DRIVER={ODBC Driver 17 for SQL Server};"
       "SERVER=(localdb)\\MSSQLLocalDB;"
       "DATABASE=Prod_Data;"
       "Trusted_Connection=yes;"
       "Encrypt=no;" 
    )
    return pyodbc.connect(conn_str)


def run_query(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a read-only SQL query and return a list of dicts."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql, params)
        cols = [c[0] for c in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(cols, row)) for row in rows]
    finally:
        cursor.close()
        conn.close()
