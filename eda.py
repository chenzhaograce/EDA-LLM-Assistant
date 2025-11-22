from data_connector import DataConnector
dc = DataConnector()

# Local files
# df = dc.read_csv('data.csv')
# df = dc.read_excel('data.xlsx', sheet_name='Sheet1')
# df = dc.auto_detect_and_read('data.csv')  # Auto-detects format

# SQLite
sqlite_path = 'ad_campaign_db.sqlite'  # Update with your actual .sqlite file
tables = dc.list_sqlite_tables(sqlite_path)
print(f"\n📋 Found {len(tables)} tables in '{sqlite_path}': {tables}")

# Preview each table (first 5 rows) to decide which one to load
table_previews = {}
for table_name in tables:
	preview_query = f"SELECT * FROM {table_name} LIMIT 5"
	preview_df = dc.read_sqlite_query(sqlite_path, preview_query)
	table_previews[table_name] = preview_df
	print(f"\n🔎 Preview of table '{table_name}' (first 5 rows):")
	print(preview_df)

# Choose a table to load after inspecting previews
target_table = tables[0] if tables else None
if target_table:
	print(f"\n✅ Loading full table: {target_table}")
	df = dc.read_sqlite_table(sqlite_path, target_table)
	print(f"Loaded DataFrame shape: {df.shape}")
else:
	df = None
	print("⚠️ No tables found in the SQLite database.")

# Example custom query (adjust as needed)
if df is not None and 'customers' in tables:
	df_custom_query = dc.read_sqlite_query(sqlite_path, 'SELECT * FROM customers WHERE age > 30')
	print("\n🎯 Custom query result (customers age > 30):")
	print(df_custom_query.head())

# # MySQL
# df = dc.read_mysql('localhost', 'mydb', 'user', 'password', table='customers')

# # PostgreSQL  
# df = dc.read_postgresql('localhost', 'mydb', 'user', 'password', 
#                        query='SELECT * FROM customers LIMIT 1000')

# # BigQuery
# df = dc.read_bigquery('my-project', query='SELECT * FROM `dataset.table`')