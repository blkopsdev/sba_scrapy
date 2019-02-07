import scrapy
import re
import MySQLdb
import traceback

class NaicsSpider(scrapy.Spider):

    def __init__(self):
        self.conn = MySQLdb.connect(
            host='localhost',
            user='root',
            passwd='root',
            db='sba',
            charset="utf8",
            use_unicode=True
        )
        self.cursor = self.conn.cursor()

    name = "naics"
    allowed_domains = ['https://www.naics.com']
    start_urls = ['https://www.naics.com/six-digit-naics/?code=54']

    def parse(self, response):
        trs = response.xpath(
            '//table[contains(@class, "table-striped")]//tr[contains(@class, "groupRow")]'
        )
        
        for tr in trs:
            naics = tr.xpath('./following-sibling::td[@class="first-child"]/text()').extract_first()

            try:
                self.cursor.execute("""SELECT id FROM naics WHERE naics = $s""", (naics,))
                naics_id = self.cursor.fetchall()
            except MySQLdb.Error, e:
                print traceback.format_exc()

            if not naics_id:
                try:
                    self.cursor.execute("""INSERT INTO naics (naics) VALUES (%s)""", (naics,))

                except MySQLdb.Error, e:
                print traceback.format_exc()
