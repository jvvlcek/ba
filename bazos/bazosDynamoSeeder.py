from pymongo import MongoClient
import boto3
from dotenv import load_dotenv
import sys
import os
print("\n".join(sys.path))
sys.path.append(os.path.abspath("C:/Users/Aitonin/Kaufio"))

from BE.Scrappers.utils import db_connections as kaufioDB

load_dotenv()

# Connect to MongoDB
mongo_uri = kaufioDB.mongo_uri
mongo_db_name = kaufioDB.mongo_db_name
mongo_collection_name_scrap = kaufioDB.mongo_collection_scrap
mongo_collection_name_live = kaufioDB.mongo_collection_live
client = MongoClient(mongo_uri)
db = client[mongo_db_name]
collection_scrap = db[mongo_collection_name_scrap]
collection_live = db[mongo_collection_name_live]

# Access environment variables
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
dynamodb_table_name = os.getenv('DYNAMODB_TABLE_NAME')

print(aws_access_key)

# Connect to DynamoDB
dynamo_client = boto3.resource('dynamodb', region_name=aws_region)
dynamo_table = dynamo_client.Table(dynamodb_table_name)

def batch_write_to_dynamo(items):
    try:
        with dynamo_table.batch_writer() as batch:
            for item in items:
                batch.put_item(Item=item)
        print(f"Bazos: Batch of {len(items)} items inserted successfully.")
        return True
    except Exception as e:
        print(f"Bazos: Batch Error: {e}")
        return False

def move_transferred_items_to_live(transferred_items):
    try:
        for item in transferred_items:
            # Update 'movedToDynamo' to True in the original collection
            collection_scrap.update_one(
                {'url': item['url']},
                {'$set': {'movedToDynamo': True}}
            )

            # Insert the item into the live collection
            collection_live.insert_one(item)

            # Remove the item from the original collection
            collection_scrap.delete_one({'url': item['url']})

        print(f"Bazos: Successfully moved {len(transferred_items)} items to the live collection.")
    except Exception as e:
        print(f"Bazos: Error while moving items to the live collection: {e}")

batch_size = 25  # Number of items to process in each batch
max_items = 0    # Maximum number of items to transfer (0 for no limit)
current_count = 0
buffer = []
cursor = collection_scrap.find({"images_downloaded": True}).batch_size(batch_size)


# Iterate over MongoDB documents
seen_keys = set()  # Track unique key combinations to avoid duplicates

for document in cursor:
    # Dynamically generate scraper and updated keys
    scraper_key = str(document.get('_id', 'no-id'))  # Use 'origin' as scraper key

    # Check for duplicates in the current batch
    if (scraper_key) in seen_keys:
        print(f"Duplicate detected: scraper={scraper_key}")
        continue  # Skip duplicate items

    seen_keys.add((scraper_key))  # Add the key combination to the set

    date1 = str(document.get('advert_created', ''))
    date2 = date1.replace("'", "").replace("b", "").split(".")
    date3 = date2[2]+"-"+date2[1]+"-"+date2[0]
    finaldate = str(date3)

    # Prepare DynamoDB item
    buffer.append({
        'tenant': 'bazos',
        'id': str(document.get('_id', '')),
        'scraper': scraper_key,  # Partition key
        'advert_created': finaldate,
        'url': document.get('url', ''),
        'title': document.get('title', ''),
        'price': document.get('price', 0),
        'description': document.get('description', ''),
        'location_details': document.get('location_details', []),
        'categories': document.get('categories', []),
        'tokens': document.get('tokens', []),
        'images': document.get('images', []),
        'kaufio_images': document.get('kaufio_images', []),
        'inserted': str(document.get('created', '1970-01-01T00:00:00Z')),
        'image': '',
        'type': document.get('type', ''),
        'alive': bool(document.get('alive', 1)),
        'flagged': False,
    })

    current_count += 1

    # Stop processing if max_items is set and reached
    if max_items > 0 and current_count >= max_items:
        print(f"Bazos: Reached the maximum limit of {max_items} items.")
        break

    # Write batch to DynamoDB when buffer is full
    if len(buffer) == batch_size:
        if batch_write_to_dynamo(buffer):
            move_transferred_items_to_live(buffer)
            buffer = []  # Clear the buffer
            seen_keys.clear()  # Clear the set for the next batch
        else:
            print("Bazos: Error occurred. Halting further execution.")
            break

# Write any remaining items in the buffer
if buffer:
    if batch_write_to_dynamo(buffer):
        move_transferred_items_to_live(buffer)
        print("Bazos: Last batch successfully inserted.")
    else:
        print("Bazos: Error occurred while inserting the last batch.")
