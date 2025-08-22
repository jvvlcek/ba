from scrapy.item import Item, Field

class KaufiospiderItem(Item):
    item_url = Field()
    allids = Field()

class KaufioSpiderList(Item):
    yieldDict: list = KaufiospiderItem()