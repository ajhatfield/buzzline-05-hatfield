import io
import csv
import sys
import time
import json
import pathlib
import os
import tempfile
import pandas as pd
from collections import defaultdict
from utils.utils_logger import logger
import utils.utils_config as config

# Function to process a single message and calculate sentiment
def process_message(message):
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

# Function to collect and process messages in memory
def consume_messages_from_file(live_data_path, interval_secs):
    author_sentiment = defaultdict(list)
    
    logger.info(f"Consuming messages from file: {live_data_path}")
    
    try:
        with open(live_data_path, 'r') as file:
            for line in file:
                if line.strip():
                    message = json.loads(line.strip())  # Parse JSON line
                    processed_message = process_message(message)
                    
                    if processed_message:
                        # Collect sentiment by author
                        author_sentiment[processed_message['author']].append(processed_message['sentiment'])
                        
            logger.info("Successfully processed messages.")
            
            # After processing, calculate the average sentiment for each author
            avg_sentiment = {
                author: sum(sentiments) / len(sentiments) for author, sentiments in author_sentiment.items()
            }
            logger.info(f"Average sentiment per author: {avg_sentiment}")

            # Display the result in memory (you can print or log it)
            display_result_in_memory(avg_sentiment)
            
    except FileNotFoundError:
        logger.error(f"ERROR: Live data file not found at {live_data_path}.")
        sys.exit(10)
    except Exception as e:
        logger.error(f"ERROR: Error reading from live data file: {e}")
        sys.exit(11)

    # Sleep before checking for new messages
    time.sleep(interval_secs)

# Function to display the calculated result in memory
def display_result_in_memory(avg_sentiment):
    # Create a virtual CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)

    # Write headers
    writer.writerow(['Author', 'Average Sentiment'])

    # Write average sentiment for each author
    for author, sentiment in avg_sentiment.items():
        writer.writerow([author, sentiment])

    # Go to the beginning of the virtual CSV
    output.seek(0)

    # Create a temporary file and write the CSV content into it
    with tempfile.NamedTemporaryFile(delete=False, mode='w', newline='', suffix='.csv') as temp_file:
        temp_file.write(output.getvalue())  # Write the CSV content
        temp_file_path = temp_file.name
        logger.info(f"Temporary CSV created at: {temp_file_path}")
    
    # Open the temporary CSV file using the default CSV viewer (e.g., Excel)
    open_csv_file(temp_file_path)

# Function to open the CSV file with the default viewer
def open_csv_file(temp_file_path):
    try:
        if sys.platform == "win32":  # If running on Windows
            os.startfile(temp_file_path)
        elif sys.platform == "darwin":  # If running on MacOS
            os.system(f"open {temp_file_path}")
        else:  # For Linux and other platforms
            os.system(f"xdg-open {temp_file_path}")
        logger.info(f"Opened CSV file: {temp_file_path}")
    except Exception as e:
        logger.error(f"Error opening CSV file: {e}")
        print(f"Error opening CSV file: {e}")

# Main function to drive the process
def main():
    logger.info("Starting Consumer to process messages and calculate average sentiment.")

    try:
        interval_secs = int(config.get_message_interval_seconds_as_int())
        live_data_path = pathlib.Path(config.get_live_data_path())  # Ensure it's a Path object
        logger.info(f"Live data file path: {live_data_path}")
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    # STEP 2. Ensure the live data file exists
    if not live_data_path.exists():
        logger.error(f"ERROR: Live data file not found at {live_data_path}.")
        sys.exit(10)

    logger.info("STEP 3. Begin consuming and processing messages.")
    try:
        consume_messages_from_file(live_data_path, interval_secs)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error during message consumption: {e}")
    finally:
        logger.info("Consumer shutting down.")
