import requests
from pymongo import MongoClient
from datetime import datetime
import xml.etree.ElementTree as ET

# MongoDB connection details
client = MongoClient("mongodb://localhost:27017/")
db = client["kaufio_local"]
sitemap_collection = db['bazos_sitemap_urls']
extracted_collection = db['bazos_extracted_urls']
extracted_collection.drop()
# Go through all URLs in 'bazos_sitemap_urls' collection
sitemaps = sitemap_collection.find({}, {"url": 1})

for sitemap in sitemaps:
    sitemap_url = sitemap['url']
    try:
        # Fetch the HTML content of the sitemap
        response = requests.get(sitemap_url)

        if response.status_code == 200:
            # Parse the XML content
            root = ET.fromstring(response.content)

            # Find all <loc> tags and extract the URLs
            urls = [loc.text for loc in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')]

            # Current timestamp
            timestamp = datetime.utcnow()

            # Prepare data to insert into the 'bazos_extracted_urls' collection
            data_to_insert = [{'url': url, 'timestamp': timestamp, 'sitemap': sitemap_url, 'processingstatus': 0} for url in urls]

            # Insert the extracted URLs into MongoDB
            if data_to_insert:
                result = extracted_collection.insert_many(data_to_insert)
                print(f"Inserted {len(result.inserted_ids)} URLs from {sitemap_url}")
            else:
                print(f"No URLs found in {sitemap_url}")
        else:
            print(f"Failed to fetch {sitemap_url}. Status code: {response.status_code}")

    except Exception as e:
        print(f"An error occurred while processing {sitemap_url}: {e}")
