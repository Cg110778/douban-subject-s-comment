# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo
from douban4.items import *

class MongoPipeline(object):
    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DB')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]
        #self.db[DoubanuserItem.collection].create_index([('id', pymongo.ASCENDING)])

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):

        if isinstance(item, DoubandetailmovieItem):
            self.db[item.collection].update({'movie_id': item.get('movie_id')}, {'$set': item}, True)
        if isinstance(item, DoubandetailmoviecommentItem) :
            self.db[item.collection].update(
                {'movie_id': item.get('movie_id')},
                {'$addToSet':
                    {
                        'movie_comment_info':  {'$each':item['movie_comment_info']}
                    }
                }, True)#电影


        if isinstance(item, DoubandetailmusicItem):
            self.db[item.collection].update({'music_id': item.get('music_id')}, {'$set': item}, True)
        if isinstance(item, DoubandetailmusiccommentItem) :
            self.db[item.collection].update(
                {'music_id': item.get('music_id')},
                {'$addToSet':
                    {
                        'music_comment_info':  {'$each':item['music_comment_info']}
                    }
                }, True)#音乐




        if isinstance(item, DoubandetailbookItem):
            self.db[item.collection].update({'book_id': item.get('book_id')}, {'$set': item}, True)
        if isinstance(item, DoubandetailbookcommentItem) :
            self.db[item.collection].update(
                {'book_id': item.get('book_id')},
                {'$addToSet':
                    {
                        'book_comment_info':  {'$each':item['book_comment_info']},
                    }
                }, True)#书籍


        return item

