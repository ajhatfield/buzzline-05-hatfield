import json
import pathlib
import sys
import time
import sqlite3
from pathlib import Path
import utils.utils_config as config
from utils.utils_logger import logger

# Helper function to initialize the database
def init_db(db_path: Path):
    """Initialize the SQLite database with tables."""
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        # Create the 'messages' table to store the messages
        c.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY,
                message TEXT,
                author TEXT,
                timestamp TEXT,
                category TEXT,
                sentiment REAL,
                keyword_mentioned TEXT,
                message_length INTEGER
            )
        ''')
        
        # Create the 'author_sentiment' table to store average sentiment per author
        c.execute('''
            CREATE TABLE IF NOT EXISTS author_sentiment (
                author TEXT PRIMARY KEY,
                average_sentiment REAL
            )
        ''')
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized at {db_path}")
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize database: {e}")
        sys.exit(1)

# Function to insert a message into the database
def insert_message(message: dict, db_path: Path):
    """Insert a processed message into the 'messages' table."""
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        # Insert message into the messages table
        c.execute('''
            INSERT INTO messages (message, author, timestamp, category, sentiment, keyword_mentioned, message_length)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (message['message'], message['author'], message['timestamp'], message['category'], 
              message['sentiment'], message['keyword_mentioned'], message['message_length']))

        conn.commit()
        conn.close()
        logger.info(f"Message inserted for author {message['author']}")
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message: {e}")

# Function to calculate the average sentiment for each author
def calculate_average_sentiment(db_path: Path):
    """Calculate the average sentiment per author and insert/update in the 'author_sentiment' table."""
    try:
        conn = sqlite3.connect(db_path)
        c = conn.cursor()

        # Get average sentiment for each author
        c.execute('''
            SELECT author, AVG(sentiment) AS avg_sentiment
            FROM messages
            GROUP BY author
        ''')
        
        authors_avg_sentiment = c.fetchall()
        logger.info(f"Average Sentiment Data: {authors_avg_sentiment}")

        # Update or insert the average sentiment for each author
        for author, avg_sentiment in authors_avg_sentiment:
            c.execute('''
                INSERT OR REPLACE INTO author_sentiment (author, average_sentiment)
                VALUES (?, ?)
            ''', (author, avg_sentiment))

        conn.commit()
        conn.close()
        logger.info("Average sentiment for authors updated.")
    except Exception as e:
        logger.error(f"ERROR: Failed to calculate average sentiment: {e}")

# Function to process a single message
def process_message(message: str) -> dict:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
            "message_length": int(message.get("message_length", 0)),
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None

# Function to consume messages from the file and process them
def consume_messages_from_file(live_data_path, db_path, interval_secs, last_position):
    """
    Consume new messages from a file and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - live_data_path (pathlib.Path): Path to the live data file.
    - db_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval in seconds to check for new messages.
    - last_position (int): Last read position in the file.
    """
    logger.info("Called consume_messages_from_file() with:")
    logger.info(f"   {live_data_path=}")
    logger.info(f"   {db_path=}")
    logger.info(f"   {interval_secs=}")
    logger.info(f"   {last_position=}")

    while True:
        try:
            logger.info(f"3. Read from live data file at position {last_position}.")
            with open(live_data_path, "r") as file:
                file.seek(last_position)
                for line in file:
                    if line.strip():
                        message = json.loads(line.strip())
                        processed_message = process_message(message)
                        if processed_message:
                            insert_message(processed_message, db_path)

                last_position = file.tell()
                calculate_average_sentiment(db_path)  # Calculate and update average sentiment

                return last_position

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found at {live_data_path}.")
            sys.exit(10)
        except Exception as e:
            logger.error(f"ERROR: Error reading from live data file: {e}")
            sys.exit(11)

        time.sleep(interval_secs)

# Main function to run the consumer process
def main():
    """
    Main function to run the consumer process.
    Reads configuration, initializes the database, and starts consumption.
    """
    logger.info("Starting Consumer to run continuously.")
    
    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path_str: str = config.get_live_data_path()
        live_data_path: Path = Path(live_data_path_str)
        db_path: Path = Path('sentiment.db')  # Set the path for the sentiment database
        logger.info(f"SUCCESS: Read environment variables. Live data path: {live_data_path}")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Initialize the database.")
    try:
        init_db(db_path)  # Initialize the sentiment.db database
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize database: {e}")
        sys.exit(2)

    logger.info("STEP 3. Begin consuming and storing messages.")
    try:
        consume_messages_from_file(live_data_path, db_path, interval_secs, 0)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Consumer shutting down.")

if __name__ == "__main__":
    main()
