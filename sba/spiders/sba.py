import scrapy
from urlparse import urljoin
import re
import json


class SbaSpider(scrapy.Spider):
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


        for tr in trs:
            datum = {}
            for idx, id in enumerate(ids):
                value = tr.xpath('.//td[contains(@headers, "{}")]/text()'.format(id)).extract_first()
                datum[keys[idx]] = value

            response.meta.update({
                'profile_table': datum
            })
            link = tr.xpath('.//a/@href').extract_first()

            yield scrapy.Request(urljoin(response.url, link), callback=self.get_data, dont_filter=True,
                                 meta=response.meta)



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