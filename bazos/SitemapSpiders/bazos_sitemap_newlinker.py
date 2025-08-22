import requests
import xml.etree.ElementTree as ET
from pymongo import MongoClient
from datetime import datetime

# MongoDB connection details
client = MongoClient("mongodb://localhost:27017/")
db = client["kaufio_local"]
collection = db['bazos_extracted_urls_newlinker']
collection_old = db['bazos_extracted_urls']
sitemap_collection = db['bazos_sitemap_urls']


def getSitemaps():
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

    collection_sitemaps = db['bazos_sitemap_urls']

    # Drop the 'bazos_sitemap_urls' collection before inserting new data
    collection_sitemaps.drop()

    # Iterate through each base URL and process the corresponding sitemap
    for base_url in allsitemaps:
        sitemap_url2 = base_url + 'sitemap.php'
        print(f"Processing sitemap: {sitemap_url2}")

        try:
            # Fetch the sitemap
            response1 = requests.get(sitemap_url2)

            # Check if the request was successful
            if response1.status_code == 200:
                # Parse the XML content
                root = ET.fromstring(response1.content)

                # Find all <loc> tags and extract the URLs containing "sitemapdetail.php"
                urls2 = [loc.text for loc in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                        if "sitemapdetail.php" in loc.text]

                # Current timestamp
                timestamp2 = datetime.utcnow()

                # Prepare data for MongoDB
                data_to_insert2 = [{'url': url, 'timestamp': timestamp2, 'sitemap': sitemap_url2} for url in urls2]

                # Insert the data into MongoDB
                if data_to_insert2:
                    result2 = collection_sitemaps.insert_many(data_to_insert2)
                    print(f"Downloaded and inserted {len(result2.inserted_ids)} URLs from {sitemap_url2}")
                else:
                    print(f"No URLs containing 'sitemapdetail.php' found in {sitemap_url2}")
            else:
                print(f"Failed to fetch the sitemap: {sitemap_url2}. Status code: {response.status_code}")

        except Exception as e:
            print(f"An error occurred while processing {sitemap_url2}: {e}")

    print("Sitemap crawling completed.")

getSitemaps()
# Drop the 'bazos_sitemap_urls_newlinker' collection before inserting new data
collection.drop()

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
                result = collection.insert_many(data_to_insert)
                print(f"Inserted {len(result.inserted_ids)} URLs from {sitemap_url}")
            else:
                print(f"No URLs found in {sitemap_url}")
        else:
            print(f"Failed to fetch {sitemap_url}. Status code: {response.status_code}")

    except Exception as e:
        print(f"An error occurred while processing {sitemap_url}: {e}")



print("Sitemap crawling completed.")

# Start to dif them:
urls_new = set(url['url'] for url in collection.find({}, {'url': 1}))
urls_old = set(url['url'] for url in collection_old.find({}, {'url': 1}))
fresh_urls = urls_new - urls_old
print("Fresh URLS found: ", len(fresh_urls))
# insert urls_new to collection_old
data_to_insert = [{'url': url, 'processingstatus': 0} for url in fresh_urls]
result = collection_old.insert_many(data_to_insert)
print("Fresh URLs inserted")