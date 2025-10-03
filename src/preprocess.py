import pandas as pd
import sqlite3
from database import DatabaseManager

class DataPreprocessor:
    def __init__(self, db_path="recommendations.db"):
        """
        Initializes the data preprocessor
        db_path: path to the SQLite database
        """

        self.db_path = db_path
        self.db_manager = DatabaseManager(db_path)
    
    def load_data_from_db(self):
        """
        Loads data from the SQLite database
        Returns: DataFrame with the items
        """

        try:
            self.db_manager.connect()
            cursor = self.db_manager._get_cursor()

            # Load all items from the database
            query = "SELECT id, title, genre, year, description FROM items"
            items_df = pd.read_sql_query(query, self.db_manager.connection)

            print(f"Loaded {len(items_df)} items from the database")
            return items_df
        except Exception as e:
            print(f"Error loading data from database: {e}")
            return pd.DataFrame()
        finally:
            self.db_manager.close()
    
    def clean_data(self, df):
        """
        Cleans the DataFrame
        - Handles null values
        - Normalizes genres
        - Normalizes years
        """
        print("Starting data cleanup...")

        # Verify that df is a pandas DataFrame
        if not isinstance(df, pd.DataFrame):
            raise ValueError("The df parameter must be a pandas DataFrame")
        
        # Verify that the DataFrame is not empty
        if df.empty:
            print("Empty DataFrame, no data to clean")
            return df

        # Create a copy to avoid modifying the original
        cleaned_df = df.copy()

        #1. Handle null values ​​in title (required field)
        if "title" in cleaned_df.columns and cleaned_df["title"].isnull().any():
            print('Null titles found, filling with "Unknown Title"')
            cleaned_df["title"] = cleaned_df["title"].fillna("Unknown Title")
        
        # 2. Handling null values ​​in genres
        if "genre" in cleaned_df.columns and cleaned_df['genre'].isnull().any():
            print('Null genres found, filling with "Unknown"')
            cleaned_df['genre'] = cleaned_df['genre'].fillna('Unknown')

        # 3. Handle null values ​​in year
        if 'year' in cleaned_df.columns and cleaned_df['year'].isnull().any():
            print("Null years found, filling with 0")
            cleaned_df['year'] = cleaned_df['year'].fillna(0)
            # Convert to integer
            cleaned_df['year'] = cleaned_df['year'].astype(int)
        
        # 4. Handle null values ​​in description
        if 'description' in cleaned_df.columns and cleaned_df['description'].isnull().any():
            print("Null descriptions found, filling with empty string")
            cleaned_df['description'] = cleaned_df['description'].fillna('')
        
        print("Limpieza de valores nulos completada")
        return cleaned_df