import sqlite3
import threading

class DatabaseManager:
    _thread_local = threading.local()

    def __init__(self, db_path='crawler_database.db'):
        """
        Initialize database connection with thread-local storage
        """
        self.db_path = db_path
        self.initialize_database()

    def _get_connection(self):
        """
        Get thread-local database connection
        """
        if not hasattr(self._thread_local, 'connection'):
            self._thread_local.connection = sqlite3.connect(self.db_path)
        return self._thread_local.connection

    def _get_cursor(self):
        """
        Get thread-local cursor
        """
        connection = self._get_connection()
        if not hasattr(self._thread_local, 'cursor'):
            self._thread_local.cursor = connection.cursor()
        return self._thread_local.cursor

    def initialize_database(self):
        """
        Create database tables if they don't exist
        """
        try:
            connection = self._get_connection()
            cursor = connection.cursor()

            # Create URLs table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS urls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    url TEXT UNIQUE,
                    content TEXT,
                    crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    status TEXT DEFAULT 'pending'
                )
            ''')

            # Create links table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS links (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_url TEXT,
                    target_url TEXT,
                    crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            connection.commit()
        except sqlite3.Error as e:
            print(f"Database initialization error: {e}")

    def insert_url(self, url, content=None, status='pending'):
        """
        Insert or update URL in the database
        """
        try:
            cursor = self._get_cursor()
            connection = self._get_connection()
            
            cursor.execute('''
                INSERT OR REPLACE INTO urls (url, content, status) 
                VALUES (?, ?, ?)
            ''', (url, content, status))
            connection.commit()
            return cursor.lastrowid
        except sqlite3.Error as e:
            print(f"Error inserting URL: {e}")
            return None

    def insert_link(self, source_url, target_url):
        """
        Insert link between URLs
        """
        try:
            cursor = self._get_cursor()
            connection = self._get_connection()
            
            cursor.execute('''
                INSERT OR REPLACE INTO links (source_url, target_url) 
                VALUES (?, ?)
            ''', (source_url, target_url))
            connection.commit()
        except sqlite3.Error as e:
            print(f"Error inserting link: {e}")

    def get_pending_urls(self, limit=10):
        """
        Retrieve pending URLs to crawl
        """
        try:
            cursor = self._get_cursor()
            cursor.execute('''
                SELECT url FROM urls 
                WHERE status = 'pending' 
                LIMIT ?
            ''', (limit,))
            return [row[0] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            print(f"Error retrieving pending URLs: {e}")
            return []

    def update_url_status(self, url, status):
        """
        Update status of a specific URL
        """
        try:
            cursor = self._get_cursor()
            connection = self._get_connection()
            
            cursor.execute('''
                UPDATE urls SET status = ? 
                WHERE url = ?
            ''', (status, url))
            connection.commit()
        except sqlite3.Error as e:
            print(f"Error updating URL status: {e}")

    def close_connections(self):
        """
        Close all thread-local database connections
        """
        if hasattr(self._thread_local, 'connection'):
            try:
                self._thread_local.connection.close()
                del self._thread_local.connection
                del self._thread_local.cursor
            except Exception as e:
                print(f"Error closing connection: {e}")