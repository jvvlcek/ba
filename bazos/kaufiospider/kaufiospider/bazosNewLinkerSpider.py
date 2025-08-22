import scrapy
from items import KaufiospiderItem
import categorizer_bazos

pageCap = {
                  "https://zvirata.bazos.cz/": 100,
                  "https://deti.bazos.cz/": 50,
                  "https://reality.bazos.cz/": 60,
                  "https://prace.bazos.cz/": 30,
                  "https://auto.bazos.cz/": 500,
                  "https://motorky.bazos.cz/": 60,
                  "https://stroje.bazos.cz/": 90,
                  "https://dum.bazos.cz/": 90,
                  "https://pc.bazos.cz/": 100,
                  "https://mobil.bazos.cz/": 90,
                  "https://foto.bazos.cz/": 40,
                  "https://elektro.bazos.cz/": 60,
                  "https://sport.bazos.cz/": 100,
                  "https://hudba.bazos.cz/": 60,
                  "https://vstupenky.bazos.cz/": 30,
                  "https://knihy.bazos.cz/": 40,
                  "https://nabytek.bazos.cz/": 50,
                  "https://obleceni.bazos.cz/": 70,
                  "https://sluzby.bazos.cz/": 20,
                  "https://ostatni.bazos.cz/": 200,
}
class BazosnewlinkerspiderSpider(scrapy.Spider):
    name = "bazosCrawler"
    start_urls = ["https://zvirata.bazos.cz/",
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
                  "https://ostatni.bazos.cz/",
                  ]

    xPaths = {
        "categoryLink": '//div[contains(@class, "barvaleva")]/a[not(contains(@href, "http"))]/@href',
        "categoryName": '//h1[contains(@class, "nadpiskategorie")]/text()',
        "nextPage": '//div[contains(@class, "strankovani")]/a/b[contains(text(), "Další")]/../@href',
        "itemUrl": '//h2[contains(@class, "nadpis")]/a/@href',
        "mainCat": '//div[contains(@class, "drobky")]/a[2]/text()',
        "curretPage": '//span[contains(@class, "cisla")]/text()'
    }
    custom_settings = {
        'DOWNLOAD_DELAY': 5,  # Set the download delay to 1 second
    }

    def my_errback(self, failure):
        # Handle the error here
        self.logger.error(repr(failure))

    def parse(self, response):
        # Extract category links
        category_links = response.xpath(self.xPaths["categoryLink"]).getall()

        # Follow each category link
        for category_link in category_links:
            yield response.follow(category_link, callback=self.parse_category)

    def parse_category(self, response):
        spider_item = KaufiospiderItem()
        yieldDict = []
        # Extract the category name for the current category page
        category_name = response.xpath(self.xPaths["categoryName"]).get()
        self.logger.info(f"Category Name: {category_name}")

        mainCatName = response.xpath(self.xPaths["mainCat"]).get()
        self.logger.info(f"Main category Name: {mainCatName}")
        # Use the category dictionary to map the category name to its ID
        category_info = categorizer_bazos.categorization_bazos.get(mainCatName)

        if category_info is not None:
            category_id = category_info.get("id")
            # Extract item URLs on the category page
            item_urls = response.xpath(self.xPaths["itemUrl"]).getall()

            self.logger.info(f"Main category ID: {category_id}")
            # Declaring MAIN CAT ID, so we can put it in the DB through META > YIELD
            mainidstring = str(category_id)

            # Declaring nested dictionary main_cat_items

            categorization_bazos_data = categorizer_bazos.categorization_bazos.get(mainCatName, {}).get(
                "main_cat_items", {})
            if mainCatName == "Reality":
                # Blank dictionary so we can merge MULTICASTED later on
                selected_data = {}
                # In case of MULTI-NESTED category (for Bazos its only Reality) we will just use the last level and merge it
                # in one dictionary, so we can iterate through it later on and determine the sub_cat_id
                if "Prodej" in categorization_bazos_data:
                    selected_data.update(categorization_bazos_data["Prodej"])
                if "Pronájem" in categorization_bazos_data:
                    selected_data.update(categorization_bazos_data["Pronájem"])

                # Iterate through the MULTI-NESTED dictionary, so we can determine the sub_cat_id
                # Use category_name to iterate through the dict, when it finds match, it returns its value (number id)
                for key, value in selected_data.items():
                    if category_name == key:
                        sub_cat_id = value  # value is the sub-category ID corresponding to the category_name
                        subidstring = str(sub_cat_id)

                for index, item_url in enumerate(item_urls, start=0):
                    self.logger.info(f"Found item URL within category: {item_url}")
                    spider_item['item_url'] = item_url
                    allids = subidstring + "," + mainidstring
                    spider_item['allids'] = allids
                    # Debug logging to check item_url
                    self.logger.info(f"Processing item URL: {item_url}")
                    self.logger.info(f"FRICK SPIDER ITEM URL: {spider_item['item_url']}")
                    self.logger.info(f"SPIDER IDS: {spider_item['allids']}")
                    #
                    yieldDict.append(spider_item.deepcopy())
                    self.logger.info(f"spider_item= {spider_item}")
                    self.logger.info(f"YieldDict= {yieldDict}")

                    self.logger.info(f"Index: {index}")
                    if index % 19 == 0:
                        yield {'items': yieldDict}
                        self.logger.info(f"Append DICT")
                        yieldDict.clear()
            else:
                for key, value in categorizer_bazos.categorization_bazos.get(mainCatName, {}).get('main_cat_items',
                                                                                                  {}).items():
                    if category_name == key:
                        sub_cat_id = value  # value is the sub-category ID corresponding to the category_name
                        subidstring = str(sub_cat_id)
                for index, item_url in enumerate(item_urls, start=0):
                    self.logger.info(f"Found item URL within category: {item_url}")
                    spider_item['item_url'] = item_url
                    allids = subidstring + "," + mainidstring
                    spider_item['allids'] = allids
                    # Debug logging to check item_url
                    self.logger.info(f"Processing item URL: {item_url}")
                    # Here you can see all spider_item shit we need to put in the DB
                    self.logger.info(f"FRICK SPIDER ITEM URL: {spider_item['item_url']}")
                    self.logger.info(f"SPIDER IDS: {spider_item['allids']}")
                    # Instead of yielding a request here, yield the spider_item
                    # It is just impossible to yield a request because of next_page
                    # If you are going to use request, you are going to die
                    yieldDict.append(spider_item.deepcopy())

                    self.logger.info(f"spider_item= {spider_item}")
                    self.logger.info(f"YieldDict= {yieldDict}")
                    self.logger.info(f"Index: {index}")

                    if index % 19 == 0:
                        yield {'items': yieldDict}
                        self.logger.info(f"Append DICT")
                        yieldDict.clear()


            # Extract the main category from the URL
            main_category_url = 'https://' + response.url.split('/')[2] + '/'
            self.logger.info(f"Current URL: {response.url}")
            self.logger.info(f"Main Category URL: {main_category_url}")

            # Get the page cap for the current category
            max_pages = pageCap.get(main_category_url, 0)   
            self.logger.info(f"Page cap for {main_category_url}: {max_pages}")

            # Extract the current page number
            current_page = int(response.xpath(self.xPaths["curretPage"]).get().split('/')[0])
            self.logger.info(f"Current Page: {current_page}")

        # Check if there is a "next page" button and follow it
        # next_page = response.xpath(self.xPaths["nextPage"]).get()
        # if next_page:
        #     yield response.follow(next_page, callback=self.parse_category)

            if current_page < max_pages:
                next_page = response.xpath(self.xPaths["nextPage"]).get()
                if next_page:
                    yield response.follow(next_page, callback=self.parse_category)
                else:
                    self.logger.info("No more pages to follow in this category.")
            else:
                self.logger.info("Reached page cap for this category.")