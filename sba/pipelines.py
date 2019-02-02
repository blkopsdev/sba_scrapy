# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
import traceback

class SbaPipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect(
            host='localhost',
            user='root',
            passwd='root',
            db='scrapy',
            charset="utf8",
            use_unicode=True
        )
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        method = getattr(self, spider.name)
        return method(item)

    def sba(self, item):
        try:
            self.cursor.execute(
                """INSERT INTO economic_group (naics_id, state, economic_group, num_of_firms) 
                VALUES (%s, %s, %s, %s)""", (
                    item['E-mail Address:'],
                    item['WWW Page:'],
                    item['E-Commerce Website:'],
                    item['Contact Person:'],
                    item['County Code (3 digit):'],
                    item['Congressional District:'],
                    item['Metropolitan Statistical Area:'],
                    item['CAGE Code:'],
                    item['Year Established:'],
                    item['Naicses'],
                    item['keywords'],
                    item['naics_codes'],
                    item['state']
                )
            )

            self.conn.commit()

        except MySQLdb.Error, e:

            print("Error %d: %s" % (e.args[0], e.args[1]))

        return item