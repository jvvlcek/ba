import threading
import requests
import time
import sys
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
from BE.Scrappers.utils import db_connections as kaufioDB

# Load environment variables
load_dotenv()

# MongoDB Configuration
client = MongoClient(kaufioDB.mongo_uri)
db = client[kaufioDB.mongo_db_name]
scraped_collection = db[kaufioDB.mongo_collection_scrap]
archived_collection = db[kaufioDB.mongo_collection_archived]

# Settings
BATCH_SIZE = 50000
CHECK_INTERVAL = 10  # Check internet connection every 10 seconds

def is_internet_connected():
    """
    Check if the internet connection is active by pinging a reliable server.
    """
    try:
        requests.get("https://www.google.com", timeout=5)
        return True
    except requests.RequestException:
        return False

def monitor_internet():
    """
    Monitor the internet connection and terminate the program if disconnected.
    """
    while True:
        if not is_internet_connected():
            print("Bazos: Internet connection lost. Terminating the scraper...")
            sys.exit(1)
        time.sleep(CHECK_INTERVAL)

def fetch_adverts(batch_size):
    twenty_four_hours_ago = datetime.utcnow() - timedelta(hours=24)
    return scraped_collection.find(
        {
            "alive": True,
            "$or": [
                {"last_checked": {"$exists": False}},
                {"last_checked": {"$lt": twenty_four_hours_ago}}
            ]
        }
    ).limit(batch_size)

def check_advert_status(advert):
    url = advert["url"]
    try:
        response = requests.head(url, timeout=5)
        status_code = response.status_code

        if status_code != 200 or 201:
            print(f"Bazos: Advert {url} is not active. Archiving...")
            advert["alive"] = False
            archived_collection.insert_one(advert)
            scraped_collection.delete_one({"_id": advert["_id"]})
            print("Bazos: Advert deleted from scraped collection.")
        else:
            print(f"Bazos: Advert {url} is active. Status code: {status_code}")
            scraped_collection.update_one(
                {"_id": advert["_id"]},
                {"$set": {"last_checked": datetime.utcnow()}}
            )
    except Exception as e:
        print(f"Bazos: Error checking URL {url}: {e}")

def process_adverts():
    start_time = time.time()
    adverts = fetch_adverts(BATCH_SIZE)

    for advert in adverts:
        check_advert_status(advert)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Bazos: Processing completed in {elapsed_time:.2f} seconds.")

if __name__ == "__main__":
    print("Bazos: Starting the Archivator...")

    # Start the internet monitor thread
    internet_thread = threading.Thread(target=monitor_internet, daemon=True)
    internet_thread.start()

    # Main process
    try:
        process_adverts()
        print("Bazos: Archivator processing completed.")
    except KeyboardInterrupt:
        print("Bazos: Interrupted by user. Exiting...")
        sys.exit(0)
