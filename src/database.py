import sqlite3
import pandas as pd
import os
from pathlib import Path

class DatabaseManager:
    def __init__(self, db_path="recommendations.db"):
        """
        Initializes the database manager
        db_path: path where the SQLite database will be created
        """

        self.db_path = db_path
        self.connection = None
    
    def connect(self):
        """ Establishes connection to the database """

        try:
            self.connection = sqlite3.connect(self.db_path)

            # Enable foreign keys
            self.connection.execute("PRAGMA foreign_keys = ON")
            print(f"Connected to the database: {self.db_path}")
            return self.connection
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            return None
        
    def _get_cursor(self):
        """
        Safe method to get a cursor.
        Verify that the connection exists first.
        """

        if self.connection is None:
            print("No connection to the database. Connecting...")
            self.connect()
        
        if self.connection is None:
            raise Exception("Could not establish connection to the database")
        
        return self.connection.cursor()
    
    def _safe_commit(self):
        """
        Safe method to commit
        Verify that the connection exists first
        """

        if self.connection is None:
            print("No connection to commit")
            return False
        
        try:
            self.connection.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error committing: {e}")
            return False
    
    def create_tables(self):
        """ Create the necessary tables in the database """

        # SQL to create user table
        users_table_sql = """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # SQL to create table of items (animes)
        items_table_sql = """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            genre TEXT,
            year INTEGER,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        # SQL to create ratings table
        ratings_table_sql = """
        CREATE TABLE IF NOT EXISTS ratings (
            user_id INTEGER,
            item_id INTEGER,
            rating REAL NOT NULL CHECK (rating >= 0 AND rating <= 5),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, item_id),
            FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
            FOREIGN KEY (item_id) REFERENCES items (id) ON DELETE CASCADE
        )
        """

        try:
            cursor = self._get_cursor()

            # Run table creation
            cursor.execute(users_table_sql)
            cursor.execute(items_table_sql)
            cursor.execute(ratings_table_sql)

            if self._safe_commit():
                print("Tables created successfully")
            else:
                print("Error creating tables - commit failed")

        except sqlite3.Error as e:
            print(f"Error creating tables: {e}")
        except Exception as e:
            print(f"Unexpected error: {e}")
    
    def insert_initial_data(self, csv_path="data/animes.csv"):
        """
        Insert initial data from a CSV file
        csv path: path to the CSV file with the item data
        """

        # Check if the CSV file exists and has data.
        create_sample_data = False

        if not os.path.exists(csv_path):
            print(f"CSV file not found: {csv_path}")
            create_sample_data = True
        else:
        
            try:
                # Read the CSV file
                df = pd.read_csv(csv_path)
                print(f"Reading data from: {csv_path}")
                print(f"Found {len(df)} records")

                # Check if CSV has data (not just headers)
                if len(df) == 0:
                    print("CSV file is empty, creating sample data...")
                    create_sample_data = True
                else:
                    cursor = self._get_cursor()
                    items_inserted = 0

                    for _, row in df.iterrows():
                        try:
                            title = row.get("title", row.get("name", 'Untitled'))
                            genre = row.get("genre", row.get("genres", 'Unknown'))
                            year = row.get("year", row.get("release_year", None))
                            description = row.get("description", row.get("synopsis", ''))

                            cursor.execute(
                                "INSERT OR IGNORE INTO items (title, genre, year, description) VALUES (?, ?, ?, ?)", (title, genre, year, description)
                            )

                            items_inserted += 1
                        except Exception as e:
                            print(f"Error inserting item: {e}")
                            continue
                    
                    if self._safe_commit():
                        print(f"Inserted {items_inserted} items into the database")
                    else:
                        print("Error inserting data - commit failed")
                        return
                    
                    # Create sample users and ratings
                    self._create_sample_users()
                    self._create_sample_ratings
            except Exception as e:
                print(f"Error reading CSV file: {e}")
                create_sample_data = True
        
        # Create sample data if CSV doesn't exist or is empty
        if create_sample_data:
            print("Creating sample data...")
            self._create_sample_data()
        
    def _create_sample_users(self):
        """Create some sample users"""
        sample_users = [
            "Example user 1",
            "Example user 2",
            "Example user 3"
        ]

        try:
            cursor = self._get_cursor()

            for user in sample_users:
                cursor.execute("INSERT OR IGNORE INTO users (name) VALUES (?)", (user,))
        
            if self._safe_commit():
                print("Example users created")
            else:
                print("Error creating users - commit failed")

        except Exception as e:
            print(f"Error creating users: {e}")

    def _create_sample_ratings(self):
        """ Create sample ratings for testing """
        try:
            cursor = self._get_cursor()

            # Get all users and items
            users = cursor.execute("SELECT id FROM users").fetchall()
            items = cursor.execute("SELECT id FROM items").fetchall()

            if not users or not items:
                print("There is not enough data to create example ratings.")
                return
            
            # Create some random ratings
            import random
            ratings_created = 0

            for user_id in [u[0] for u in users]:
                # Each user rates some random items
                sample_items = random.sample([i[0] for i in items], min(5, len(items)))

                for item_id in sample_items:
                    rating  = round(random.uniform(3.0, 5.0), 1)

                    cursor.execute(
                        "INSERT OR IGNORE INTO ratings (user_id, item_id, rating) VALUES (?, ?, ?)", (user_id, item_id, rating)
                    )

                    ratings_created += 1

            if self._safe_commit():
                print(f"Created {ratings_created} sample ratings")
            else:
                print("Error creating ratings - commit failed")

        except Exception as e:
            print(f"Error creating ratings: {e}")
    
    def _create_sample_data(self):
        """ Create sample data if there is no CSV """

        sample_items = [
            ("Naruto", "Action, Adventure", 2002, "Young ninja seeks to become Hokage"),
            ("Attack on Titan", "Action, Drama", 2013, "Humans fight against titans"),
            ("Death Note", "Mystery, Thriller", 2006, "Book that kills by writing names"),
            ("My Hero Academia", "Action, Superheroes", 2016, "Young man without powers in a world of heroes"),
            ("Demon Slayer", "Action, Fantasy", 2019, "Young Demon Hunter")
        ]

        try:
            cursor = self._get_cursor()

            for item in sample_items:
                cursor.execute(
                    "INSERT INTO items (title, genre, year, description) VALUES (?, ?, ?, ?)", item
                )
            
            if self._safe_commit():
                print(f"Sample data created")

                # Create sample users and ratings
                self._create_sample_users()
                self._create_sample_ratings()
            else:
                print("Error creating sample data - commit failed")
         
        except Exception as e:
            print(f"Error creating sample data: {e}")
    
    def close(self):
        """ Closes the connection to the database """

        if self.connection:
            self.connection.close()
            print(f"Database connection closed")
            self.connection = None  # Important: reset to None
        
def initialize_database():
    """ Main function to initialize the entire database """

    # Create data directory if it doesn't exist
    Path("data").mkdir(exist_ok=True)

    db_manager = DatabaseManager()

    if db_manager.connect():
        db_manager.create_tables()
        db_manager.insert_initial_data()
        db_manager.close()
    else:
        print("The database could not be initialized.")

if __name__ == "__main__":
    # If we run this file directly, it initializes the database
    initialize_database()