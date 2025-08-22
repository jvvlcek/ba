import requests
import xml.etree.ElementTree as ET
from pymongo import MongoClient
from datetime import datetime

# List of base URLs
allsitemaps = [
    "https://zvirata.bazos.cz/",
    "https://deti.bazos.cz/",
    "https://reality.bazos.cz/",
    "https://prace.bazos.cz/",
    "https://auto.bazos.cz/",
    "https://motorky.bazos.cz/",
    "https://stroje.bazos.cz/",
    "https://dum.bazos.cz/",
    "https://pc.bazos.cz/",
    "https://mobil.bazos.cz/",
    "https://foto.bazos.cz/",
    "https://elektro.bazos.cz/",
    "https://sport.bazos.cz/",
    "https://hudba.bazos.cz/",
    "https://vstupenky.bazos.cz/",
    "https://knihy.bazos.cz/",
    "https://nabytek.bazos.cz/",
    "https://obleceni.bazos.cz/",
    "https://sluzby.bazos.cz/",
    "https://ostatni.bazos.cz/"
]

# MongoDB connection details
client = MongoClient("mongodb://localhost:27017/")
db = client["kaufio_local"]
collection = db['bazos_sitemap_urls']

# Drop the 'bazos_sitemap_urls' collection before inserting new data
collection.drop()

# Iterate through each base URL and process the corresponding sitemap
for base_url in allsitemaps:
    sitemap_url = base_url + 'sitemap.php'
    print(f"Processing sitemap: {sitemap_url}")

    try:
        # Fetch the sitemap
        response = requests.get(sitemap_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Parse the XML content
            root = ET.fromstring(response.content)

            # Find all <loc> tags and extract the URLs containing "sitemapdetail.php"
            urls = [loc.text for loc in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                    if "sitemapdetail.php" in loc.text]

            # Current timestamp
            timestamp = datetime.utcnow()

            # Prepare data for MongoDB
            data_to_insert = [{'url': url, 'timestamp': timestamp, 'sitemap': sitemap_url} for url in urls]

            # Insert the data into MongoDB
            if data_to_insert:
                result = collection.insert_many(data_to_insert)
                print(f"Downloaded and inserted {len(result.inserted_ids)} URLs from {sitemap_url}")
            else:
                print(f"No URLs containing 'sitemapdetail.php' found in {sitemap_url}")
        else:
            print(f"Failed to fetch the sitemap: {sitemap_url}. Status code: {response.status_code}")

    except Exception as e:
        print(f"An error occurred while processing {sitemap_url}: {e}")

print("Sitemap crawling completed.")
