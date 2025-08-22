import sys
import os

print("\n".join(sys.path))
sys.path.append(os.path.abspath("C:/Users/vojta/Desktop/bazos/bazos"))


import requests
from lxml import html
from datetime import datetime
import re
import socket
import time
from BE.Scrappers.utils import functions, db_connections as kaufioDB, xpaths as xpath
from pymongo import MongoClient
from BE.Scrappers.bazos.kaufiospider.kaufiospider.categorizer_bazos import categorization_bazos
from BE.Scripts.Tokenizer.tokenizerBazos import create_tags
import multiprocessing



"""
Processingstatus:
0 = Not yet processed
1 = Allocated by worker
2 = Processing
3 = Completed (inserted into DB)

"""
# MongoDB configuration
mongo_uri = kaufioDB.mongo_uri
mongo_db_name = kaufioDB.mongo_db_name
mongo_collection_name_crawl = kaufioDB.mongo_collection_crawl
mongo_collection_name_scrap = kaufioDB.mongo_collection_scrap

client = MongoClient(mongo_uri)
db = client[mongo_db_name]
collection_crawl = db[mongo_collection_name_crawl]
collection_scrap = db[mongo_collection_name_scrap]


def find_matching_key(breadcrumbs, categorization):
    categories = []
    mainCatName = breadcrumbs[0]
    category_info = categorization_bazos.get(mainCatName)
    if category_info is not None:
        categoryName = breadcrumbs[-1]
        category_id = category_info.get("id")
        categories.append(category_id)

        categorization_bazos_data = categorization.get(mainCatName, {}).get( "main_cat_items", {})
        if mainCatName == "Reality":
            selected_data = {}
            if "Prodej" in categorization_bazos_data:
                selected_data.update(categorization_bazos_data["Prodej"])
            if "Pronájem" in categorization_bazos_data:
                selected_data.update(categorization_bazos_data["Pronájem"])
            for key, value in selected_data.items():
                if categoryName == key:
                    sub_cat_id = value  # value is the sub-category ID corresponding to the category_name
                    categories.append(sub_cat_id)
        else:
            for key, value in categorization.get(mainCatName, {}).get('main_cat_items',{}).items():
                if categoryName == key:
                    sub_cat_id = value  # value is the sub-category ID corresponding to the category_name
                    categories.append(sub_cat_id)
        if len(categories) > 0:
            return categories
    else:
        print("Found no Category Info")

urlDict = {
    "https://zvirata.bazos.cz": 347,
    "https://sluzby.bazos.cz": 346,
    "https://sport.bazos.cz": 108,
    "https://obleceni.bazos.cz": 272,
    "https://reality.bazos.cz": 150,
    "https://prace.bazos.cz": 160,
    "https://foto.bazos.cz": 262,
    "https://mobil.bazos.cz": 196,
    "https://pc.bazos.cz": 211,
    "https://elektro.bazos.cz": 184,
    "https://nabytek.bazos.cz": 118,
    "https://dum.bazos.cz": 109,
    "https://knihy.bazos.cz": 93,
    "https://vstupenky.bazos.cz": 92,
    "https://hudba.bazos.cz": 86,
    "https://ostatni.bazos.cz": 78,
    "https://deti.bazos.cz": 67,
    "https://motorky.bazos.cz": 52,
    "https://stroje.bazos.cz": 64,
    "https://auto.bazos.cz": 2
}


# Scrape functions
def getTitle(titleXPath):
    # Returns page title
    if len(titleXPath) > 0:
        title = titleXPath[0].encode('utf-8').decode('utf-8')  # Ensure it's a string
        clean_title = functions.clean_text(title)
        return clean_title
    else:
        return 0


def getPrice(priceXPath):
    # Returns clean number
    if len(priceXPath) > 0:
        number = functions.dirtyStr2DigitsOnly(priceXPath[0])
        if isinstance(number, int):
            return number
        else:
            return 0
    else:
        return 0


def getDescription(descriptionXPath):
    if len(descriptionXPath) > 0:
        description1 = descriptionXPath[0]
        # print("description1: ", description1)
        html_string = html.tostring(description1, encoding="utf-8", method="html").decode("utf-8")
        # print("html_string: ", html_string)
        match = re.search(r'<div class="popisdetail">(.*?)<\/div>', html_string, flags=re.DOTALL)
        if match:
            result_text = match.group(1)
            # print("result_text: ", result_text)
            return functions.deleteMail(result_text)
        else:
            return "Bez popisku"  # or some default value or error message
    else:
        return 0


def getLocation(locationXPath):
    locationList = []
    # Returns PSC, CITY
    if len(locationXPath) > 0:
        psc = locationXPath[0]
        city = locationXPath[1]
        locationList.append(psc)
        locationList.append(city)
        return locationList
    else:
        return 0


def getImages(imagesXPath):
    imgList = []
    if len(imagesXPath) > 0:
        for image in imagesXPath:
            cleanImage = image.xpath('@data-flickity-lazyload')
            imgUrl = cleanImage[0].split("?")
            # replace https://www.bazos.cz/img
            imgUrl2 = imgUrl[0]
            imgList.append(imgUrl2)
        return imgList if len(imgList) > 0 else 0
    else:
        return 0


def getDateCreated(dateCreatedXPath1, dateCreatedXPath):
    if len(dateCreatedXPath1) > 0:
        date = dateCreatedXPath1[0].replace(' ', '').replace('[', '').replace(']', '').replace('-', '')
        return date
    elif len(dateCreatedXPath) > 0:
        date2 = dateCreatedXPath[0].replace(' ', '').replace('[', '').replace(']', '').replace('-', '')
        date3 = date2.encode('utf-8')
        return date3
    else:
        return 0


def getBreadcrumbs(breadcrumbsXPath):
    breadcrumbsList = []
    if len(breadcrumbsXPath) > 0:
        for i in breadcrumbsXPath:
            breadcrumbsList.append(i)
        # Join the list into a string (no encoding needed)
        breadcrumbs =  breadcrumbsList

        if len(breadcrumbs) > 0:
            return breadcrumbs  # Return as regular string
        else:
            return 0  # If empty, return 0
    else:
        return 0  # If breadcrumbsXPath is empty, return 0


def getAdvertID(advertIDXPath):
    return functions.dirtyStr2DigitsOnly(advertIDXPath[0]) if len(advertIDXPath) > 0 else 0


def getAlive(aliveXPath):
    return False if len(aliveXPath) > 0 else True


# THE MAIN LOGIC HERE #################################################################################
# Let's save ads by 10-50 items and not 1 by 1 like a retard
# Browser agent for requests
agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/604.5.6 (KHTML, like Gecko) Version/11.0.3 Safari/604.5.6"
headers = {'User-Agent': agent}
itemDict = {}
itemList = []

def getUrlsToScrape():
    try:
        # Fetch the result list; limit to X items
        fetchResultList = list(collection_crawl.find({'processingstatus': 0}).limit(100))
        if fetchResultList:
            print("fetchResultList: ", fetchResultList)
            # Extract the IDs to update processing status
            ids_to_update = [result['_id'] for result in fetchResultList]
            # Update "processingStatus" to 1 for these documents
            collection_crawl.update_many(
                {'_id': {'$in': ids_to_update}},
                {'$set': {'processingstatus': 1}}  # Ensure field name is consistent
            )
            print("Updated processing status for fetched results to 1 (processing).")
        return fetchResultList
    except Exception as e:
        print("Error:", str(e))


def scrapeTenAds(fetchResultList):
    for result in fetchResultList:
        # Requests setup
        p = requests.get(result['url'], headers=headers)
        p.encoding = 'UTF-8'
        tree = html.fromstring(p.text)
        print("result url", result['url'])
        # XPaths
        xpathTitle = tree.xpath(xpath.bazos['title'])
        xpathPrice = tree.xpath(xpath.bazos['price'])
        xpathDescription = tree.xpath(xpath.bazos['description'])
        xpathLocation = tree.xpath(xpath.bazos['location'])
        xpathImages = tree.xpath(xpath.bazos['images'])
        xpathDateCreated = tree.xpath(xpath.bazos['dateCreated'])
        xpathDateCreated1 = tree.xpath(xpath.bazos['dateCreated1'])
        xpathBreadcrumbs = tree.xpath(xpath.bazos['breadcrumbs'])
        xpathAlive = tree.xpath(xpath.bazos['alive'])

        # Get categories
        print("Breadcrumbs: ", getBreadcrumbs(xpathBreadcrumbs))
        categories1 = find_matching_key(getBreadcrumbs(xpathBreadcrumbs), categorization_bazos)
        categories = categories1[-1]

        # Check if the ad is alive and has a title
        if getAlive(xpathAlive) and getTitle(xpathTitle) != 0:
            # Generate tokens
            title = getTitle(xpathTitle)
            description = getDescription(xpathDescription)
            tokens = create_tags(title, description)  # Generate tokens using the Tokenizer
            token_values = [token[0] for token in tokens]  # Extract just the token words
            # print ("Token values: ", token_values)
            # Prepare the ad item
            itemDict = {
                'url': result['url'],  # Full ad URL
                'origin': 'bazos',
                'title': title,  # Title without specials
                'price': getPrice(xpathPrice),  # Price in INT
                'description': description,  # Description string HTML
                'location_details': getLocation(xpathLocation),  # JSON of location details > PSC + town
                'categories': categories,  # Category information
                'images': getImages(xpathImages),  # Image URLs in list
                "kaufio_images": [],
                "images_downloaded": False,
                'path': getBreadcrumbs(xpathBreadcrumbs),  # Breadcrumbs / category path
                'type': 'SALE',  # Assume bazos is sale only
                'alive': True,  # T / F
                'advert_created': getDateCreated(xpathDateCreated1, xpathDateCreated),
                'tokens': token_values,  # Store generated tokens here
                "created": datetime.utcnow(),
                "movedToDynamo": False,
            }

            itemList.append(itemDict)
            print("Set processing status to 2 for: ", result['url'])

            # Update MongoDB status
            try:
                collection_crawl.update_one(
                    {'_id': result['_id']},
                    {'$set': {'processingstatus': 2}}
                )
                print("Update to processingstatus 2 (scraped) was successful")
            except Exception as e:
                print("Error:", str(e))
        else:
            # Remove dead ads
            try:
                collection_crawl.delete_one({'_id': result['_id']})
                print("Deleting " + result['url'] + " because it's dead..")
            except Exception as e:
                print("Error:", str(e))

    if len(itemList) >= 1:
        return itemList
    else:
        print(f" (worker {worker_id}) No ads in the itemList")
        return None




def insertTenAdsInDB(itemList):
    print("Starting insertTenAdsInDB function..")
    if len(itemList) > 0:
        # print("itemList: ", itemList)
        # Update processing status to 3
        insertMarker = 1
        for result in itemList:
            url = result['url']
            insertMarker += 1
            if insertMarker % 100 == 0:
                insertMarker = 1
                print("50th iterration")
            # Set processing status to "completed"
            try:
                collection_crawl.update_one(
                    {'result': url},
                    {'$set': {'processingstatus': 3}}
                )
                print("Update to 3 was Successful.")
            except Exception as e:
                print("Error:", str(e))
        # Commit everything in the DB
        collection_scrap.insert_many(itemList)
        print("Successfully added 50 items to the MongoDB...")
        # Clear the list
        print("Added", len(itemList), " items")
        print("Clearing itemList....")
        itemList.clear()


def is_server_running():
    try:
        # Try to connect to the server's socket
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(1)  # Set a timeout for the connection attempt
        client_socket.connect(("127.0.0.1", 6000))
        message_to_server = "Hello from Client 1!"
        client_socket.send(message_to_server.encode("utf-8"))
        client_socket.close()
        return True
    except (ConnectionRefusedError, socket.timeout):
        return False



def main_logic(worker_id):
    while is_server_running():
        print(f" (worker {worker_id}) Server is running, starting scraping.....")
        urlsToScrape = getUrlsToScrape()
        listOfData = scrapeTenAds(urlsToScrape)
        if listOfData:
            insertTenAdsInDB(listOfData)
            print(f" (worker {worker_id}) All commited....")
        else:
            print(f" (worker {worker_id}) Nic more....")
        itemList.clear()

if __name__ == "__main__":
    num = 7 #num of workers

    workers = []

    for worker_id in range(num):
        worker = multiprocessing.Process(target=main_logic, args=(worker_id,))
        worker.start()
        workers.append(worker)

        time.sleep(5)

    for worker in workers:
        worker.join()