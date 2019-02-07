import scrapy
from urlparse import urljoin
import re
import json
import MySQLdb
import traceback

class SbaSpider(scrapy.Spider):

    def __init__(self):
        self.conn = MySQLdb.connect(
            host='localhost',
            user='root',
            passwd='root',
            db='sba_scrapy',
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
        try:
            self.cursor.execute("""SELECT naics FROM naics""")
            naicses = self.cursor.fetchall()

            for naics in naicses:
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
        except MySQLdb.Error, e:
            print traceback.format_exc()


    def parse_search(self, response):
        economic_group = response.xpath('//div[contains(@class, "qmshead") and a[@href]]')

        for g in economic_group:
            key = g.xpath('.//a/text()').extract_first()
            number_of_firms = g.xpath('./following-sibling::div[contains(@class, "qmsinfo")]/a[@href]/text()').re_first(r'\d+')
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
                        """SELECT id FROM economics WHERE naics_id = %s AND economic_group = %s AND state = %s""", (naics_id, key, state,)
                    )
                    ecom = self.cursor.fetchall()

                    if not ecom:
                        try:
                            self.cursor.execute(
                                """INSERT INTO economics ( economic_group, naics_id, state, num_of_firms)
                                VALUES (%s, %s, %s, %s)""", (
                                    key,
                                    naics_id,
                                    state,
                                    number_of_firms
                                )
                            )
                            self.conn.commit()
                            self.cursor.execute(
                                """SELECT id FROM economics WHERE naics_id = %s AND economic_group = %s AND state = %s""", (naics_id, key, state,)
                            )
                            ecom = self.cursor.fetchall()
                            economic_id = ecom[0][0]

                        except MySQLdb.Error, e:
                            print traceback.format_exc()
                    else:
                        economic_id = ecom[0][0]

                    response.meta.update({
                        'naics_id': naics_id,
                        'economic_id': economic_id
                    })
                except:
                    print traceback.format_exc()
            economig_group = g.xpath('.//a/text()').extract_first()
            response.meta.update({
                'economic_group': economig_group
            })
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
            '//table[@id="ProfileTable"]//tr[contains(@class, "AlternatingRowBGC4Form")]'
        )

        economic_id = response.meta.get('economic_id')

        for tr in trs:
            datum = {}
            for idx, id in enumerate(ids):
                value = tr.xpath('.//td[contains(@headers, "{}")]/text()'.format(id)).extract_first()
                if not value:
                    value = tr.xpath('.//td[contains(@headers, "{}")]/a/text()'.format(id)).extract_first()
                    link = tr.xpath('.//td[contains(@headers, "{}")]/a[@href]/@href').extract_first()
                    # duns = re.search(r'DUNS=\.(.*?)', link)
                datum[keys[idx]] = value

            list_id = None

            try:
                self.cursor.execute(
                    """SELECT id FROM profilelists WHERE contact = %s AND economic_id = %s""",
                    (datum['Contact'], economic_id,)
                )
                list = self.cursor.fetchall()
            except:
                print traceback.format_exc()
                print "Error: unable to fecth profile list data"

            try:
                if not list:
                    self.cursor.execute(
                        """INSERT INTO profilelists ( trade_name, contact, address, capabilities, economic_id, naics_id, state)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)""", (
                            datum['Name and Trade Name of Firm'],
                            datum['Contact'],
                            datum['Address and City, State Zip'],
                            datum['Capabilities Narrative'],
                            economic_id,
                            response.meta.get('naics_id'),
                            response.meta.get('State')
                        )
                    )
                    self.conn.commit()

                    self.cursor.execute(
                        """SELECT id FROM profilelists WHERE contact = %s AND economic_id = %s""",
                        (datum['Contact'], economic_id,)
                    )
                    list = self.cursor.fetchall()
                    list_id = list[0][0]
                else:
                    list_id = list[0][0]
            except:
                print traceback.format_exc()
                print "Error: unable to fecth profile list data"
            else:
                response.meta.update({
                    'list_id': list_id
                })

                link = tr.xpath('.//a/@href').extract_first()
                yield scrapy.Request(urljoin(response.url, link), callback=self.get_data, dont_filter=True,
                                 meta=response.meta)

    def get_data(self, response):

        keywords = response.xpath('//h3[contains(text(), "Keywords")]/following-sibling::div[@class="indent_same_as_profilehead"]/text()').extract_first().strip()
        office = response.xpath('//h3[contains(text(), "Business Development Servicing Office")]/following-sibling::div[@class="indent_same_as_profilehead"]/text()').extract_first().strip()
        capabilities = response.xpath('//h3[contains(text(), "Capabilities Narrative:")]/following-sibling::div[@class="indent_same_as_profilehead"]/text()').extract_first().strip()
        # naics_codes = response.xpath('//table[@summary="NAICS Codes"]//tr//td[contains(@headers, "C2")]/text()').extract()
        ids = response.xpath(
            '//table[@summary="NAICS Codes"]//tr//th[@id and text()]/@id'
        ).extract()
        keys = response.xpath(
            '//table[@summary="NAICS Codes"]//tr//th[@id and text()]/text()'
        ).extract()
        trs = response.xpath(
            '//table[@summary="NAICS Codes"]//tbody//tr'
        )
        naics_data = None
        for tr in trs:
            datum = {}
            for idx, id in enumerate(ids):
                value = tr.xpath('.//td[contains(@headers, "{}")]/text()'.format(id)).extract_first()
                datum[keys[idx]] = value
                naics_data = json.dumps(datum)

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
            'keywords': keywords.encode('utf-8'),
        }

        data = response.xpath('//div[@class="profileline" and position() != 1]')
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
        try:
            list_id = response.meta.get('list_id')
            naics = response.meta.get('naics')

            self.cursor.execute("""SELECT id FROM profiles WHERE email = %s AND naics = %s""", (info['E-mail Address:'], naics, ))
            profile = self.cursor.fetchall()
            if not profile:
                try:
                    self.cursor.execute(
                        """INSERT INTO profiles (user_id, name_of_firm, trade_name, duns_num, p_dunms_num, address1, address2, \
                        city, state, zip, phone, fax, email, www_page, ecom_website, county_code, cong_district, ms_area, cage_code,\
                        established_year, gsa_contact, ownership, sba_8a_num, sba_8a_ent, sba_8a_exit,\
                        ishubzone_cert, jv_ent, jv_exit, naics_table, keywords, performance_history, list_id,\
                        quality_assurance, electronic_data, export_business_activity, exporting_to, bonding_agg, bonding_cont, \
                        con_bonding_agg, con_bonding_cont, accept_card, desired_export_business, export_descrption, business_office, naics, economic, contact, capabilities) VALUES (%s, %s, %s, %s,\
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,\
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
                            info['User ID:'],
                            info['Name of Firm:'],
                            info['Trade Name ("Doing Business As ..."):'],
                            info['DUNS Number:'],
                            info['Parent DUNS Number:'],
                            info['Address, line 1:'],
                            info['Address, line 2:'],
                            info['City:'],
                            info['State:'],
                            info['Zip:'],
                            info['Phone Number:'],
                            info['Fax Number:'],
                            info['E-mail Address:'],
                            info['WWW Page:'],
                            info['E-Commerce Website:'],
                            info['County Code (3 digit):'],
                            info['Congressional District:'],
                            info['Metropolitan Statistical Area:'],
                            info['CAGE Code:'],
                            info['Year Established:'],
                            info['GSA Advantage Contract(s):'],
                            info['Ownership and Self-Certifications:'],
                            info['SBA 8(a) Case Number:'],
                            info['SBA 8(a) Entrance Date:'],
                            info['SBA 8(a) Exit Date:'],
                            info['HUBZone Certified?:'],
                            info['8(a) JV Entrance Date:'],
                            info['8(a) JV Exit Date:'],
                            naics_data,
                            keywords,
                            performance_json,
                            list_id,
                            info['Quality Assurance Standards:'],
                            info['Electronic Data Interchange capable?:'],
                            info['Export Business Activities:'],
                            info['Exporting to:'],
                            info['Service Bonding Level (aggregate)'],
                            info['Service Bonding Level (per contract)'],
                            info['Construction Bonding Level (aggregate)'],
                            info['Construction Bonding Level (per contract)'],
                            info['Accepts Government Credit Card?:'],
                            info['Desired Export Business Relationships:'],
                            info['Description of Export Objective(s):'],
                            office,
                            response.meta.get('naics'),
                            response.meta.get('economic_group'),
                            info['Contact Person:'],
                            capabilities
                        )
                    )
                    self.conn.commit()
                except MySQLdb.Error, e:
                    print("Error %d: %s" % (e.args[0], e.args[1]))
                    print traceback.format_exc()
        except:
            print traceback.format_exc()
        return info