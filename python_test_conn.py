import pyodbc

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=(localdb)\\MSSQLLocalDB;"
    "DATABASE=Prod_Data;"
    "Trusted_Connection=yes;"
    "Encrypt=no;"
)

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ProductionLines")
    print("✅ Connected! Rows:", cursor.fetchone()[0])
    conn.close()
except Exception as e:
    print("❌ Failed:", e)