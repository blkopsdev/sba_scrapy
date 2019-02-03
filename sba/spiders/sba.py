import scrapy
from urlparse import urljoin
import re
import json
import MySQLdb

class SbaSpider(scrapy.Spider):

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

    name = "sba"
    allowed_domains = ["sba.gov"]
    start_urls = [
        "http://web.sba.gov/pro-net/search/dsp_quicksearch.cfm"
    ]

    def parse(self, response):

        states = response.xpath('//select[@id="EltState"]/option[position()!=1]/@value').extract()
        naics = '541519'

        for state in states:
            data = {
                'AnyAllNaics': 'All',
                'naics': naics,
                'State': state
            }
            yield scrapy.FormRequest.from_response(
                response,
                formname='SearchForm',
                formdata=data,
                callback=self.parse_search,
                meta={
                    'naics': naics,
                    'State': state
                },
                dont_filter=True
            )

    def parse_search(self, response):
        econmic_group = response.xpath('//div[contains(@class, "qmshead") and a[@href]]')

        for g in econmic_group:
            key = g.xpath('.//a/text()').extract_first()
            number_of_firms = g.xpath('./following-sibling::div[contains(@class, "qmsinfo")]/a[@href]/text()').extract_first()
            if key and number_of_firms:
                response.meta.update({
                    'econmic_key': key,
                    'number_of_firms': number_of_firms
                })

                naics = response.meta.get('naics')
                state = response.meta.get('State')
                # Get Naics ID from naics table
                try:
                    self.cursor.execute(
                        """SELECT id FROM naics WHERE naics = %s""", (naics,)
                    )
                    results = self.cursor.fetchall()
                    naics_id = results[0][0]
                except:
                    print "Error: unable to fecth naics data"
                # Update or Insert econmic_group
                try:
                    self.cursor.execute(
                        """SELECT id FROM economic_group WHERE naics_id = %s AND economic_group = %s AND state = %s""", (naics_id, key, state,)
                    )
                    ecom = self.cursor.fetchall()

                    if not ecom:
                        try:
                            self.cursor.execute(
                                """INSERT INTO economic_group ( economic_group, naics_id, state, num_of_firms)
                                VALUES (%s, %s, %s, %s)""", (
                                    key,
                                    naics_id,
                                    state,
                                    number_of_firms
                                )
                            )
                            self.conn.commit()
                            self.cursor.execute(
                                """SELECT id FROM economic_group WHERE naics_id = %s AND economic_group = %s AND state = %s""", (naics_id, key, state,)
                            )
                            ecom = self.cursor.fetchall()
                            economic_id = ecom[0][0]

                        except MySQLdb.Error, e:
                            print("Error %d: %s" % (e.args[0], e.args[1]))
                    else:
                        economic_id = ecom[0][0]

                    response.meta.update({
                        'economic_id': economic_id
                    })
                    #     try:
                    #         self.cursor.execute(
                    #             """UPDATE economic_group SET num_of_firms = %s WHERE id = $d""", (number_of_firms, economic_id,)
                    #         )
                    #         self.conn.commit()
                    #     except MySQLdb.Error, e:
                    #         print("Error %d: %s" % (e.args[0], e.args[1]))

                except:
                    print "Error: unable to fecth economic group data"

            link = g.xpath('./following-sibling::div[contains(@class, "qmsinfo")]/a[@href]/@href').extract_first()
            key = re.search(r'javascript:document\.HotlinkForm\.(.*?)\.value', link)
            value = re.search(r'value = \'(.*?)\';', link)
            if not all([key, value]):
                continue
            yield scrapy.FormRequest.from_response(
                response,
                formname='HotlinkForm',
                formdata={
                    key.group(1): value.group(1)
                },
                callback=self.get_table,
                meta=response.meta,
                dont_filter=True
            )

    def get_table(self, response):
        ids = response.xpath(
            '//table[@id="ProfileTable"]//tr//th[@id and text()]/@id'
        ).extract()

        keys = response.xpath(
            '//table[@id="ProfileTable"]//tr//th[@id and text()]/text()'
        ).extract()

        trs = response.xpath(
            '//table[@id="ProfileTable"]//tr[@class="AlternatingRowBGC4Form1"]'
        )
        economic_id = response.meta.get('economic_id')

        for tr in trs:
            datum = {}
            for idx, id in enumerate(ids):
                value = tr.xpath('.//td[contains(@headers, "{}")]/text()'.format(id)).extract_first()
                datum[keys[idx]] = value

                link = tr.xpath('.//a/@href').extract_first()

                yield scrapy.Request(urljoin(response.url, link), callback=self.get_data, dont_filter=True,
                                 meta=response.meta)

            # Insert or Update profile list table
            try:
                self.cursor.execute(
                    """SELECT id FROM profile_list WHERE contact = %s AND economic_id = %s""",
                    (datum['Contact'], economic_id,)
                )
                list = self.cursor.fetchall()

                if not list:
                    try:
                        self.cursor.execute(
                            """INSERT INTO profile_list ( trade_name, contact, address, capabilities, economic_id)
                            VALUES (%s, %s, %s, %s, %s)""", (
                                datum['Name and Trade Name of Firm'],
                                datum['Contact'],
                                datum['Address and City, State Zip'],
                                datum['Capabilities Narrative'],
                                economic_id
                            )
                        )
                        self.conn.commit()

                        self.cursor.execute(
                            """SELECT id FROM profile_list WHERE contact = %s AND economic_id = %s""",
                            (datum['Contact'], economic_id,)
                        )
                        list = self.cursor.fetchall()
                        list_id = list[0][0]
                    except MySQLdb.Error, e:
                        print("Error %d: %s" % (e.args[0], e.args[1]))
                else:
                    list_id = list[0][0]

            except:
                print "Error: unable to fecth profile list data"

            response.meta.update({
                'list_id': list_id
            })

    def get_data(self, response):

        keywords = response.xpath('//h3[contains(text(), "Keywords")]/following-sibling::div[@class="indent_same_as_profilehead"]/text()').extract_first()
        naics_codes = response.xpath('//table[@summary="NAICS Codes"]//tr//td[contains(@headers, "C2")]/text()').extract()

        performance_blocks = response.xpath('//div[@class="referencebox"]')
        performances = []
        performance_json = None

        for block in performance_blocks:
            performance_elems = block.xpath('.//div[@class="profileline"]')
            performance = {}
            for elem in performance_elems:
                key = elem.xpath('.//div[@class="profilehead"]/text()').extract_first()
                value = elem.xpath('.//div[@class="profileinfo"]/text()').extract_first()

                if all([key, value]):
                   performance[key.replace(':', '')] = value

            performances.append(performance)

            performance_json = json.dumps(performances)

        info = {
            'econ': response.meta.get('econmic_key'),
            'number_of_firms': response.meta.get('number_of_firms'),
            'profile_table': response.meta.get('profile_table'),
            'naics': response.meta.get('naics', '').encode('utf-8'),
            'state_code': response.meta.get('State', '').encode('utf-8'),
            'keywords': keywords.encode('utf-8'),
            'naics_codes': ','.join(naics_codes).encode('utf-8'),
            'performances': performance_json
        }

        data = response.xpath('//div[@data-role="collapsible-set"]//div[@class="profileline" and position() != 1 and position() != 2]')
        for datum in data:
            key = datum.xpath('.//div[@class="profilehead"]//text()').extract_first()
            key = key.strip() if key else None
            if not key:
                continue
            value = datum.xpath('.//div[@class="profileinfo"]//text()').extract_first()
            value = value.strip() if value else None
            if not value:
                value = ''
            info[key] = value.encode('utf-8')

        return info