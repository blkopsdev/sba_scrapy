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
                """SELECT id FROM naics WHERE naics = %s""", (item['naics'],)
            )
            results = self.cursor.fetchall()
            for row in results:
                naics_id = row[0]

        except:
            print "Error: unable to fecth data"

        try:
            self.cursor.execute(
                """INSERT INTO economic_group ( economic_group, naics_id, state, num_of_firms)
                VALUES (%s, %s, %s, %s)""", (
                    item['econ'],
                    naics_id,
                    item['State:'],
                    item['number_of_firms']
                )
            )

            self.conn.commit()

        except MySQLdb.Error, e:

            print("Error %d: %s" % (e.args[0], e.args[1]))

        return item