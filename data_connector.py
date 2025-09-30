"""
Data Connector Module for EDA LLM Assistant

This module provides unified data loading functionality for various data sources:
- CSV, Excel, JSON files
- SQLite databases
- BigQuery
- MySQL
- PostgreSQL

Author: EDA LLM Assistant
"""

import pandas as pd
import sqlite3
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
import warnings

class DataConnector:
    """
    Unified data connector class for loading data from various sources
    """
    
    def __init__(self):
        self.connection_cache = {}
    
    def read_csv(self, path: str, **kwargs) -> pd.DataFrame:
        """
        Read CSV file
        
        Args:
            path: Path to CSV file
            **kwargs: Additional arguments for pd.read_csv()
        
        Returns:
            pandas DataFrame
        """
        try:
            print(f"📄 Reading CSV file: {path}")
            df = pd.read_csv(path, **kwargs)
            print(f"✅ Successfully loaded {df.shape[0]:,} rows and {df.shape[1]} columns")
            return df
        except Exception as e:
            print(f"❌ Error reading CSV file: {str(e)}")
            raise
    
    def read_excel(self, path: str, sheet_name: Optional[str] = None, **kwargs) -> pd.DataFrame:
        """
        Read Excel file
        
        Args:
            path: Path to Excel file
            sheet_name: Sheet name to read (default: first sheet)
            **kwargs: Additional arguments for pd.read_excel()
        
        Returns:
            pandas DataFrame
        """
        try:
            print(f"📊 Reading Excel file: {path}")
            if sheet_name:
                print(f"   Sheet: {sheet_name}")
            df = pd.read_excel(path, sheet_name=sheet_name, **kwargs)
            print(f"✅ Successfully loaded {df.shape[0]:,} rows and {df.shape[1]} columns")
            return df
        except Exception as e:
            print(f"❌ Error reading Excel file: {str(e)}")
            raise
    
    def read_json(self, path: str, **kwargs) -> pd.DataFrame:
        """
        Read JSON file
        
        Args:
            path: Path to JSON file
            **kwargs: Additional arguments for pd.read_json()
        
        Returns:
            pandas DataFrame
        """
        try:
            print(f"📋 Reading JSON file: {path}")
            df = pd.read_json(path, **kwargs)
            print(f"✅ Successfully loaded {df.shape[0]:,} rows and {df.shape[1]} columns")
            return df
        except Exception as e:
            print(f"❌ Error reading JSON file: {str(e)}")
            raise
    
    def read_sqlite_table(self, path: str, table: str, **kwargs) -> pd.DataFrame:
        """
        Read table from SQLite database
        
        Args:
            path: Path to SQLite database file
            table: Table name to read
            **kwargs: Additional arguments for pd.read_sql()
        
        Returns:
            pandas DataFrame
        """
        try:
            print(f"🗄️  Reading SQLite table '{table}' from: {path}")
            with sqlite3.connect(path) as conn:
                df = pd.read_sql(f"SELECT * FROM {table}", conn, **kwargs)
            print(f"✅ Successfully loaded {df.shape[0]:,} rows and {df.shape[1]} columns")
            return df
        except Exception as e:
            print(f"❌ Error reading SQLite table: {str(e)}")
            raise
    
    def read_sqlite_query(self, path: str, query: str, **kwargs) -> pd.DataFrame:
        """
        Execute custom SQL query on SQLite database
        
        Args:
            path: Path to SQLite database file
            query: SQL query to execute
            **kwargs: Additional arguments for pd.read_sql()
        
        Returns:
            pandas DataFrame
        """
        try:
            print(f"🔍 Executing SQLite query on: {path}")
            print(f"   Query: {query[:100]}{'...' if len(query) > 100 else ''}")
            with sqlite3.connect(path) as conn:
                df = pd.read_sql(query, conn, **kwargs)
            print(f"✅ Successfully loaded {df.shape[0]:,} rows and {df.shape[1]} columns")
            return df
        except Exception as e:
            print(f"❌ Error executing SQLite query: {str(e)}")
            raise
    
    def list_sqlite_tables(self, path: str) -> List[str]:
        """
        List all tables in SQLite database
        
        Args:
            path: Path to SQLite database file
        
        Returns:
            List of table names
        """
        try:
            with sqlite3.connect(path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [row[0] for row in cursor.fetchall()]
            print(f"📋 Found {len(tables)} tables: {tables}")
            return tables
        except Exception as e:
            print(f"❌ Error listing SQLite tables: {str(e)}")
            raise
    
    def read_mysql(self, host: str, database: str, username: str, password: str, 
                   table: str = None, query: str = None, port: int = 3306, **kwargs) -> pd.DataFrame:
        """
        Read data from MySQL database
        
        Args:
            host: MySQL server host
            database: Database name
            username: Username
            password: Password
            table: Table name to read (optional if query provided)
            query: Custom SQL query (optional if table provided)
            port: MySQL port (default: 3306)
            **kwargs: Additional arguments for pd.read_sql()
        
        Returns:
            pandas DataFrame
        """
        try:
            import pymysql
        except ImportError:
            raise ImportError("PyMySQL is required for MySQL connections. Install with: pip install pymysql")
        
        try:
            print(f"🐬 Connecting to MySQL: {host}:{port}/{database}")
            
            connection_string = f"mysql+pymysql://{username}:{password}@{host}:{port}/{database}"
            
            if query:
                print(f"   Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
                df = pd.read_sql(query, connection_string, **kwargs)
            elif table:
                print(f"   Reading table: {table}")
                df = pd.read_sql(f"SELECT * FROM {table}", connection_string, **kwargs)
            else:
                raise ValueError("Either 'table' or 'query' parameter must be provided")
            
            print(f"✅ Successfully loaded {df.shape[0]:,} rows and {df.shape[1]} columns")
            return df
        except Exception as e:
            print(f"❌ Error reading from MySQL: {str(e)}")
            raise
    
    def read_postgresql(self, host: str, database: str, username: str, password: str,
                       table: str = None, query: str = None, port: int = 5432, **kwargs) -> pd.DataFrame:
        """
        Read data from PostgreSQL database
        
        Args:
            host: PostgreSQL server host
            database: Database name
            username: Username
            password: Password
            table: Table name to read (optional if query provided)
            query: Custom SQL query (optional if table provided)
            port: PostgreSQL port (default: 5432)
            **kwargs: Additional arguments for pd.read_sql()
        
        Returns:
            pandas DataFrame
        """
        try:
            import psycopg2
        except ImportError:
            raise ImportError("psycopg2 is required for PostgreSQL connections. Install with: pip install psycopg2-binary")
        
        try:
            print(f"🐘 Connecting to PostgreSQL: {host}:{port}/{database}")
            
            connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            
            if query:
                print(f"   Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
                df = pd.read_sql(query, connection_string, **kwargs)
            elif table:
                print(f"   Reading table: {table}")
                df = pd.read_sql(f"SELECT * FROM {table}", connection_string, **kwargs)
            else:
                raise ValueError("Either 'table' or 'query' parameter must be provided")
            
            print(f"✅ Successfully loaded {df.shape[0]:,} rows and {df.shape[1]} columns")
            return df
        except Exception as e:
            print(f"❌ Error reading from PostgreSQL: {str(e)}")
            raise
    
    def read_bigquery(self, project_id: str, query: str = None, table_id: str = None, 
                     credentials_path: str = None, **kwargs) -> pd.DataFrame:
        """
        Read data from Google BigQuery
        
        Args:
            project_id: Google Cloud project ID
            query: SQL query to execute (optional if table_id provided)
            table_id: Full table ID in format 'project.dataset.table' (optional if query provided)
            credentials_path: Path to service account JSON file (optional)
            **kwargs: Additional arguments for pd.read_gbq()
        
        Returns:
            pandas DataFrame
        """
        try:
            import pandas_gbq
        except ImportError:
            raise ImportError("pandas-gbq is required for BigQuery connections. Install with: pip install pandas-gbq")
        
        try:
            print(f"☁️  Connecting to BigQuery project: {project_id}")
            
            # Set up authentication if credentials path provided
            if credentials_path:
                import os
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
                print(f"   Using credentials: {credentials_path}")
            
            if query:
                print(f"   Executing query: {query[:100]}{'...' if len(query) > 100 else ''}")
                df = pandas_gbq.read_gbq(query, project_id=project_id, **kwargs)
            elif table_id:
                print(f"   Reading table: {table_id}")
                query = f"SELECT * FROM `{table_id}`"
                df = pandas_gbq.read_gbq(query, project_id=project_id, **kwargs)
            else:
                raise ValueError("Either 'query' or 'table_id' parameter must be provided")
            
            print(f"✅ Successfully loaded {df.shape[0]:,} rows and {df.shape[1]} columns")
            return df
        except Exception as e:
            print(f"❌ Error reading from BigQuery: {str(e)}")
            raise
    
    def auto_detect_and_read(self, path: str, **kwargs) -> pd.DataFrame:
        """
        Automatically detect file type and read data
        
        Args:
            path: Path to data file
            **kwargs: Additional arguments passed to respective read functions
        
        Returns:
            pandas DataFrame
        """
        file_path = Path(path)
        extension = file_path.suffix.lower()
        
        print(f"🔍 Auto-detecting file type: {extension}")
        
        if extension == '.csv':
            return self.read_csv(path, **kwargs)
        elif extension in ['.xlsx', '.xls']:
            return self.read_excel(path, **kwargs)
        elif extension == '.json':
            return self.read_json(path, **kwargs)
        elif extension in ['.db', '.sqlite', '.sqlite3']:
            # For SQLite, we need table name
            tables = self.list_sqlite_tables(path)
            if not tables:
                raise ValueError("No tables found in SQLite database")
            if 'table' in kwargs:
                return self.read_sqlite_table(path, kwargs.pop('table'), **kwargs)
            else:
                print(f"⚠️  Multiple tables found. Reading first table: {tables[0]}")
                return self.read_sqlite_table(path, tables[0], **kwargs)
        else:
            raise ValueError(f"Unsupported file format: {extension}")

# Convenience functions for backward compatibility
def read_csv(path: str, **kwargs) -> pd.DataFrame:
    """Convenience function to read CSV"""
    connector = DataConnector()
    return connector.read_csv(path, **kwargs)

def read_excel(path: str, **kwargs) -> pd.DataFrame:
    """Convenience function to read Excel"""
    connector = DataConnector()
    return connector.read_excel(path, **kwargs)

def read_json(path: str, **kwargs) -> pd.DataFrame:
    """Convenience function to read JSON"""
    connector = DataConnector()
    return connector.read_json(path, **kwargs)

def read_sqlite_table(path: str, table: str, **kwargs) -> pd.DataFrame:
    """Convenience function to read SQLite table"""
    connector = DataConnector()
    return connector.read_sqlite_table(path, table, **kwargs)

def read_sqlite_query(path: str, query: str, **kwargs) -> pd.DataFrame:
    """Convenience function to execute SQLite query"""
    connector = DataConnector()
    return connector.read_sqlite_query(path, query, **kwargs)

def read_mysql(host: str, database: str, username: str, password: str, 
               table: str = None, query: str = None, **kwargs) -> pd.DataFrame:
    """Convenience function to read from MySQL"""
    connector = DataConnector()
    return connector.read_mysql(host, database, username, password, table, query, **kwargs)

def read_postgresql(host: str, database: str, username: str, password: str,
                   table: str = None, query: str = None, **kwargs) -> pd.DataFrame:
    """Convenience function to read from PostgreSQL"""
    connector = DataConnector()
    return connector.read_postgresql(host, database, username, password, table, query, **kwargs)

def read_bigquery(project_id: str, query: str = None, table_id: str = None, 
                 credentials_path: str = None, **kwargs) -> pd.DataFrame:
    """Convenience function to read from BigQuery"""
    connector = DataConnector()
    return connector.read_bigquery(project_id, query, table_id, credentials_path, **kwargs)

# Create global instance for easy access
data_connector = DataConnector()

if __name__ == "__main__":
    print("🔌 Data Connector Module")
    print("=" * 40)
    print("Supported data sources:")
    print("• CSV files")
    print("• Excel files (.xlsx, .xls)")
    print("• JSON files")
    print("• SQLite databases")
    print("• MySQL databases")
    print("• PostgreSQL databases")
    print("• Google BigQuery")
    print("\nExample usage:")
    print("from data_connector import DataConnector")
    print("dc = DataConnector()")
    print("df = dc.read_csv('data.csv')")