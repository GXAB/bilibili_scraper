import scrapy
from scrapy_splash import SplashRequest
import requests
import xml.etree.ElementTree as ET
import math
from datetime import date
from collections import deque

class BilibiliSpider(scrapy.Spider):
    #identity
    name = 'bilibili'

    #requests
    def start_requests(self):


        urls = [
            'https://www.bilibili.com/video/av20648403',
            'https://www.bilibili.com/video/av20310008',
            'https://www.bilibili.com/video/av18533667'
        ]
        self.link_queue = deque()
        self.headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36 OPR/60.0.3255.109'}
        for url in urls:
            yield SplashRequest(url=url, callback=self.parse, endpoint = 'render.html', args = {'wait': 7}, headers = self.headers)

    #response
    def parse(self, response):

        av = response.url.split("/")[4]
        cid = response.xpath("/html/head/script[3]/text()").extract_first()
        cid = cid.split("upgcxcode")[1].split("/")[3]
        danmu_url = "https://api.bilibili.com/x/v1/dm/list.so?oid="+cid

        comment_url = "https://api.bilibili.com/x/v2/reply"
        comment_url_params = {
                'jsonp': 'jsonp',
                'pn': 1,
                'type': '1',
                'oid': av[2:],
                'sort': '0',
            }
        page_number = requests.get(comment_url, params=comment_url_params,
            headers=self.headers).json()
        page_number = math.ceil(page_number['data']['page']['count']/20)

        def comment_extractor(reply_dict):
            document = {}
            document["av"] = av
            document["cid"] = cid
            document["Username"] = reply_dict["member"]["uname"]
            document["User_ID"] = reply_dict["member"]["mid"]
            document["Text"] = reply_dict["content"]["message"]
            document["Level"] = reply_dict["member"]["level_info"]["current_level"]
            document["Likes"] = reply_dict["like"]
            document["ctime"] = reply_dict["ctime"]
            document["Time"] = str(date.fromtimestamp(reply_dict["ctime"]))
            document["Root"] = reply_dict["root"]
            document["Parent"] = reply_dict["parent"]
            
            return document
        
        def comment_loop_extractor(replies):
            comments = []
            for comment in replies:
                comments = comments + [comment_extractor(comment)]
                if(comment["replies"] != None):
                    comments = comments + comment_loop_extractor(comment["replies"])
            return comments

        comments = []
        for number in range(page_number):
            try:
                comment_url_params["pn"] = number + 1

                replies = requests.get(comment_url, params=comment_url_params,
                    headers=self.headers).json()['data']['replies']

                comments = comments + comment_loop_extractor(replies)

            except:
                print("Unable to get comments from " + av + " page " + str(number))

        danmu_xml = requests.get(danmu_url)
        root = ET.fromstring(danmu_xml.content)

        data = {}

        data["Danmu"] = [{"av" : av, "cid" : cid, 'Metadata' : type_tag.get('p'),
            'Text' : type_tag.text} for type_tag in root.findall('d')]
        #print(comments)
        data["Comments"] = comments

        data["Video"] = {
            'av' : av,
            'cid' : cid,
            'Title' : response.xpath("//div[@id='viewbox_report']/h1/@title").extract_first(),
            'Category_1' : response.xpath("//div[@id='viewbox_report']/div[1]/span[1]/a[1]/text()").extract_first(),
            'Category_2' : response.xpath("//div[@id='viewbox_report']/div[1]/span[1]/a[2]/text()").extract_first(),
            'Date' : response.xpath("//div[@id='viewbox_report']/div[1]/span[2]/text()").extract_first(),
            'Views' : response.xpath("//div[@id='viewbox_report']/div[2]/span[1]/@title").extract_first()[4:],
            'Danmu_Count' : response.xpath("//div[@id='viewbox_report']/div[2]/span[2]/@title").extract_first()[7:],
            'Username' : response.xpath("//a[@class='username']/text()").extract_first(),
            'User_ID' : response.xpath("//a[@class='username']/@href").extract_first().split("/")[3],
            'Subscriber_Count' : response.xpath("//i[@class='van-icon-general_addto_s']/../span/text()").extract_first(),
            'Likes' : response.xpath("//div[@class='ops']/span[1]/@title").extract_first()[3:],
            'Coins' : response.xpath("//div[@class='ops']/span[2]/text()").extract_first().split()[0],
            'Favorites' : response.xpath("//div[@class='ops']/span[3]/text()").extract_first().split()[0],
            'Tags' : response.xpath("//ul[@class='tag-area clearfix']/li//text()").extract()
        }

        yield data
    
        related_videos = response.selector.xpath("//div[@class='video-page-card']/div/div[1]/div/a").extract()
        for link in related_videos:
            self.link_queue.append(link)
        yield SplashRequest(url=self.link_queue.popleft(), callback=self.parse, endpoint = 'render.html', args = {'wait': 7}, headers = self.headers)
