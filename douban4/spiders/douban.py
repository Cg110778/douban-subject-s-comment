# -*- coding: utf-8 -*-
import pymongo
import re

import scrapy
from scrapy import Request,Spider
from douban4.items import *

class DoubanSpider(Spider):
    name = 'douban4'
    allowed_domains = ['douban.com']
    start_urls = ['http://douban.com/people/{uid}/']
    contacts_url= 'https://www.douban.com/people/{uid}/contacts'
    rev_contacts_url = 'https://www.douban.com/people/{uid}/rev_contacts'
    user_url = 'http://douban.com/people/{uid}/'
    start_users = ['128290489']#'57109633',#'1693422','50446886'ninetonine''137546285'183501886'#'3452208'新桥，宋史128290489
    movie_do_url = 'https://movie.douban.com/people/{uid}/do'
    movie_wish_url ='https://movie.douban.com/people/{uid}/wish'
    movie_collect_url ='https://movie.douban.com/people/{uid}/collect'
    music_do_url ='https://music.douban.com/people/{uid}/do'
    music_wish_url ='https://music.douban.com/people/{uid}/wish'
    music_collect_url ='https://music.douban.com/people/{uid}/collect'
    book_do_url ='https://book.douban.com/people/{uid}/do'
    book_wish_url ='https://book.douban.com/people/{uid}/wish'
    book_collect_url ='https://book.douban.com/people/{uid}/collect'
    contacts_id=[]
    movie_urls=[]
    music_urls=[]
    book_urls=[]
    movie_url = 'https://movie.douban.com/subject/{movieid}/'
    music_url = 'https://music.douban.com/subject/{musicid}/'
    book_url = 'https://book.douban.com/subject/{bookid}/'


    def get_movie_id(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        movie_id=collection.find({},{'_id':0,'movie_id':1})
        return movie_id

    def get_music_id(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        music_id=collection.find({},{'_id':0,'music_id':1})
        return music_id

    def get_book_id(self,collection,database):
        client = pymongo.MongoClient('localhost', 27017)
        db = client[database]
        collection = db[collection]
        book_id=collection.find({},{'_id':0,'book_id':1})
        return book_id


    def start_requests(self):
        for uid in self.start_users:
            yield Request(self.user_url.format(uid=uid), callback=self.parse_subject)


    def parse_subject(self,response):
        movieids=self.get_movie_id(collection='movie',database='douban8')
        musicids = self.get_music_id(collection='music', database='douban8')
        bookids = self.get_book_id(collection='book', database='douban8')
        for movieid in movieids:
            if movieid:
                movieid=movieid['movie_id']
                print(movieid)
                yield Request(url=self.movie_url.format(movieid=movieid),  callback=self.parse_movie_detailink)  # 电影短评
        for musicid in musicids:
            if musicid:
                musicid=musicid['music_id']
                print(musicid)
                yield Request(url=self.music_url.format(musicid=musicid), dont_filter=True, callback=self.parse_music_detailink)
        for bookid in bookids:
            if bookid:
                bookid=bookid['book_id']
                print(bookid)
                yield Request(url=self.book_url.format(bookid=bookid), dont_filter=True, callback=self.parse_book_detailink)


    def parse_movie_detailink(self,response):
        movie_comment=response.xpath(
            '//*[@id="comments-section"]//div[@class="mod-hd"]//h2/span[@class="pl"]/a/@href').extract_first()
        # 电影短评的直接链接列表
        yield Request(url=movie_comment, callback=self.parse_movie_comment)


    def parse_music_detailink(self,response):
        music_comment=response.xpath('//div[@class="mod-hd"]//h2/span[@class="pl"]/a/@href').extract_first()
        # 音乐短评的链接列表
        yield Request(url=music_comment, callback=self.parse_music_comment)


    def parse_book_detailink(self,response):
        book_comment=response.xpath('//div[@class="mod-hd"]//h2/span[@class="pl"]/a/@href').extract_first()
        # 书籍短评的链接列表
        yield Request(url=book_comment, callback=self.parse_book_comment)


    def parse_movie_comment(self,response):
        item = DoubandetailmoviecommentItem()
        movie_id = re.search('https://movie.douban.com/subject/(.*?)/comment.*?', response.url)
        movie_id = movie_id.group(1)
        #movie_comment_url = response.url
        for movie_comment in response.xpath('//div[@id="comments"]//div[@class="comment-item"]'):
            movie_commenter_name = movie_comment.xpath(
                './/span[@class="comment-info"]//a/text()').extract_first()
            movie_commenter_id = movie_comment.xpath(
                './/span[@class="comment-info"]/a').re_first('<a href="https://www.douban.com/people/(.*?)/"')
            movie_commenter_score = movie_comment.xpath(
                './/span[@class="comment-info"]/span').re_first('<span class="allstar(\d+)0.*?</span>')
            movie_comment_time = ''.join(movie_comment.xpath(
                './/span[@class="comment-info"]//span[@class="comment-time "]/text()').extract()).strip()
            movie_comment_useful_number = movie_comment.xpath(
                './/span[@class="comment-vote"]/span[@class="votes"]/text()').extract_first()
            movie_comment_content = ''.join(movie_comment.xpath(
                './/span[@class="short"]/text()').extract()).replace('\n', '').strip()
            movie_comment_info = [
                {'movie_commenter_name': movie_commenter_name, 'movie_commenter_id': movie_commenter_id,
                 'movie_commenter_score': movie_commenter_score, 'movie_comment_time': movie_comment_time,
                 'movie_comment_useful_number': movie_comment_useful_number,
                 'movie_comment_content': movie_comment_content}]
            item['movie_id'] = movie_id
            item['movie_comment_info'] = movie_comment_info
            yield item
            movie_page = response.xpath('//*[@id="content"]//div[@class="aside"]/p/a/@href').extract_first()
            next_page = response.xpath(
                '//*[@id="paginator"]/a[@class="next"]/@href').extract_first()
            if next_page:
                if 'douban.com' in next_page:
                    yield Request(url=next_page, callback=self.parse_movie_comment)
                else:
                    next_page_url = movie_page + 'comments' + next_page
                    yield Request(url=next_page_url, callback=self.parse_movie_comment)
                    # 下一页短评列表'''


    def parse_music_comment(self, response):
        item = DoubandetailmusiccommentItem()
        music_id = re.search('https://music.douban.com/subject/(.*?)/comment.*?', response.url)
        music_id = music_id.group(1)
        #music_comment_url=response.url
        for music_comment in response.xpath('//div[@id="comments"]//li[@class="comment-item"]'):
            music_commenter_name = music_comment.xpath('.//span[@class="comment-info"]//a/text()').extract_first()
            music_commenter_id = music_comment.xpath('.//span[@class="comment-info"]/a').re_first(
                '<a href="https://www.douban.com/people/(.*?)/"')
            music_commenter_score = music_comment.xpath('.//span[@class="comment-info"]/span').re_first(
                '<span class="user-stars allstar(\d+)0.*?</span>')
            music_comment_time = ''.join(music_comment.xpath(
                './/span[@class="comment-info"]//span/text()').extract()).strip()
            music_comment_useful_number = music_comment.xpath(
                './/span[@class="comment-vote"]/span/text()').extract_first()
            music_comment_content = ''.join(music_comment.xpath('.//span[@class="short"]/text()').extract()).replace(
                '\n', '').strip()
            music_comment_info = [
                {'music_commenter_name': music_commenter_name, 'music_commenter_id': music_commenter_id,
                 'music_commenter_score': music_commenter_score, 'music_comment_time': music_comment_time,
                 'music_comment_useful_number': music_comment_useful_number,
                 'music_comment_content': music_comment_content}]
            item['music_id'] = music_id
            item['music_comment_info'] = music_comment_info
            yield item
            music_page = response.xpath('//*[@id="content"]//div[@class="aside"]//p[2]/a/@href').extract_first()
            next_page = response.xpath(
                '//*[@class="comment-paginator"]//a[contains(.,"后一页")]/@href').extract_first()  # //*[@id="paginator"]/a[@class="next"]/@href
            if next_page:
                if 'douban.com' in next_page:
                    yield Request(url=next_page, callback=self.parse_music_comment)
                else:
                    next_page_url = music_page + 'comments/' + next_page
                    yield Request(url=next_page_url, callback=self.parse_music_comment)
                    # 下一页短评列表'''

    def parse_book_comment(self, response):
        item = DoubandetailbookcommentItem()
        book_id = re.search('https://book.douban.com/subject/(.*?)/comment.*?', response.url)
        book_id = book_id.group(1)
        for book_comment in response.xpath('//div[@id="comments"]//li[@class="comment-item"]'):
            book_commenter_name = book_comment.xpath('.//span[@class="comment-info"]//a/text()').extract()
            book_commenter_id = book_comment.xpath('.//span[@class="comment-info"]/a').re_first(
                '<a href="https://www.douban.com/people/(.*?)/"')
            book_commenter_score = book_comment.xpath('.//span[@class="comment-info"]/span').re_first(
                '<span class="user-stars allstar(\d+)0.*?</span>')
            book_comment_time = ''.join(book_comment.xpath(
                './/span[@class="comment-info"]//span/text()').extract()).strip()
            book_comment_useful_number = book_comment.xpath(
                './/span[@class="comment-vote"]/span/text()').extract()
            book_comment_content = ''.join(
                book_comment.xpath('.//span[@class="short"]/text()').extract()).replace('\n', '').strip()
            book_comment_info = [
                {'book_commenter_name': book_commenter_name, 'book_commenter_id': book_commenter_id,
                 'book_commenter_score': book_commenter_score, 'book_comment_time': book_comment_time,
                 'book_comment_useful_number': book_comment_useful_number,
                 'book_comment_content': book_comment_content}]
            item['book_id'] = book_id
            item['book_comment_info'] = book_comment_info
            yield item
            book_page = response.xpath('//*[@id="content"]//div[@class="aside"]//p[2]/a/@href').extract_first()
            next_page = response.xpath(
                '//*[@class="comment-paginator"]//a[contains(.,"后一页")]/@href').extract_first()#//*[@id="paginator"]/a[@class="next"]/@href
            if next_page:
                if 'douban.com' in next_page:
                    yield Request(url=next_page, callback=self.parse_book_comment)
                else:
                    next_page_url = book_page + 'comments/' + next_page
                    yield Request(url=next_page_url, callback=self.parse_book_comment)
                    # 下一页短评列表'''



