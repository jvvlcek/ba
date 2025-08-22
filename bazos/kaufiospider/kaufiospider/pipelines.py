import mysql.connector
import mysql.connector.pooling
from scrapy.utils.project import get_project_settings

class BasicBazosCrawlUrlsPipeline(object):
    def __init__(self):
        settings = get_project_settings()
        db_config = {
            "host": settings.get('DB_HOST'),
            "user": settings.get('DB_USER'),
            "password": settings.get('DB_PASSWORD'),
            "database": settings.get('DB_NAME'),
            "pool_size": 5,  # Adjust pool size as needed
        }
        self.pool = mysql.connector.pooling.MySQLConnectionPool(**db_config)

    def process_item(self, item, spider):
        try:
            # Get a connection from the pool
            connection = self.pool.get_connection()
            cursor = connection.cursor()
            self.store_in_db(item, cursor, connection)
            spider.logger.warn(f"Stored")
            connection.commit()
            cursor.close()

        except mysql.connector.Error as err:
            # Handle the error
            self.handle_database_error(err)

        finally:
            # Release the connection back to the pool
            if connection:
                connection.close()

        return item

    def handle_database_error(self, error):
        # Implement your error handling logic here
        pass

    def store_in_db(self, item, cursor, connection):
        # Extract and join values from the list of dictionaries
        values = ", ".join([f"('{x['item_url']}', '{x['allids']}', 0)" for x in item['items']])

        # Define the INSERT query
        insert_query = f"INSERT INTO `bazosCrawledUrls` (result, allids, isscraped) VALUES {values}"
        # Execute the single INSERT statement
        cursor.execute(insert_query)

        # Commit the transaction
        connection.commit()

    def close_spider(self, spider, connection):
        if self.connection.is_connected():
            self.connection.close()


class KaufiospiderPipeline(object):

    def __init__(self):
        self.create_connection()

    def create_connection(self):
        self.conn = mysql.connector.connect(
            host='160.153.129.233',
            user='jvvlcek',
            password='6$s]YrNK#r.e',
            database='kaufio_crawl'
        )
        self.curr = self.conn.cursor()

        self.curr.execute(
            """CREATE TABLE IF NOT EXISTS `kaufio_crawl`.`bazosCrawledUrls` (
                    `id` INT NOT NULL AUTO_INCREMENT, 
                    `result` LONGTEXT NOT NULL,
                    `allids` LONGTEXT NOT NULL,
                    `isscraped` TINYINT(1) NOT NULL,
                    `date` TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
                    `processingstatus` INT(10) NOT NULL DEFAULT 0,
                    PRIMARY KEY (`id`)
                ) 
                ENGINE = InnoDB;
            """)
    def process_item(self, item, spider):
        try:
            self.store_in_db(item)
            spider.logger.warn(f"Stored")
        except Exception as e:
            spider.logger.error(f"Error storing item in the database: {str(e)}")
        return item

    # Vkladata arrays do %s
    def store_in_db(self, item):
        # Extract and join values from the list of dictionaries
        values = ", ".join([f"('{x['item_url']}', '{x['allids']}', 0)" for x in item['items']])

        # Define the INSERT query
        insert_query = f"INSERT INTO `bazosCrawledUrls` (result, allids, isscraped) VALUES {values}"
        # Execute the single INSERT statement
        self.curr.execute(insert_query)

        # Commit the transaction
        self.conn.commit()

    def close_spider(self, spider):
        if self.conn.is_connected():
            self.conn.close()


class BazosNoDuplicatesPipeline(object):

    def __init__(self):
        self.create_connection()

    def create_connection(self):
        self.conn = mysql.connector.connect(
            host='160.153.129.233',
            user='jvvlcek',
            password='6$s]YrNK#r.e',
            database='kaufio_crawl'
        )
        self.curr = self.conn.cursor(buffered=True)

    def process_item(self, item, spider):
        # Check if product_url is already in the DB
        self.curr.execute("SELECT * FROM `testspooder` WHERE result = %s", (item["item_url"],))
        answer = self.curr.fetchone()
        if answer:
            # Log the duplicate
            spider.logger.warn("Item already in database: %s" % item["item_url"])
        else:
            self.store_in_db(item)
        return item

    def store_in_db(self, items):
        # self.curr.execute(
        #     "INSERT INTO `testspooder` (result, allids, isscraped) VALUES (%s, %s, %s)", (
        #         items["item_url"],
        #         items["allids"],
        #         0
        #     ))
        # self.conn.commit()

        # Extract and join values from the list of dictionaries
        values = ", ".join([f"('{item[0]}', '{item[1]}', 0)" for item in items])

        # Define the INSERT query
        insert_query = f"INSERT INTO `testspooder` (result, allids, isscraped) VALUES {values}"

        print("QUERY: ", insert_query)
        # Execute the single INSERT statement
        self.curr.execute(insert_query)

        # Commit the transaction
        self.conn.commit()
