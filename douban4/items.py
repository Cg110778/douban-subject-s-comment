# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field,Item
class DoubanuserItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    collection = 'users'
    id = Field()
    url=Field()
    name = Field()
    img = Field()
    habitual_residence = Field()  # 常居地
    record_date = Field()  # 注册日期
    user_display = Field()  # 用户信息展示
    contacts_count = Field()  # 关注人数
    rev_contacts_count = Field()
    movie_do_number=Field()
    movie_wish_number=Field()
    movie_collect_number=Field()
    music_do_number = Field()
    music_wish_number = Field()
    music_collect_number = Field()
    book_do_number = Field()
    book_wish_number = Field()
    book_collect_number = Field()

class UserRelationItem(Item):
    collection = 'users'
    id = Field()
    contacts = Field()  # 关注
    #rev_contacts = Field()

class DoubanMovietIdItem(Item):
    collection='users'
    id=Field()
    movie_id=Field()

class DoubanMusictIdItem(Item):
    collection = 'users'
    id = Field()
    music_id = Field()
class DoubanBookIdItem(Item):
    collection = 'users'
    id = Field()
    book_id = Field()


class DoubandetailmovieItem(Item):
    collection = 'movie'
    movie_id = Field()  # 电影id
    movie_url=Field()
    movie_name=Field()#电影名字
    movie_playbill=Field()#海报
    movie_director=Field()#导演
    movie_scriptwriter=Field()#编剧
    movie_starring=Field()#主演
    movie_type=Field()#类型
    movie_producer_countryORregion=Field()#制片国家/地区
    movie_language=Field()#语言
    movie_date=Field()#上映日期
    movie_season=Field()#季数(tv)
    movie_episodes=Field()#集数(tv)
    movie_single_episode_length=Field()#单集时长(tv)
    movie_length=Field()#片长
    movie_alias=Field()#别名
    movie_IMDb=Field()#IMDb链接
    movie_star=Field()#分数
    movie_5score=Field()#5星比率
    movie_4score=Field()#4
    movie_3score=Field()#3
    movie_2score=Field()#2
    movie_1score=Field()#1
    movie_describe=Field() #简介
    movie_comment_number = Field()  # 短评数量
    #movie_review_number = Field()  # 影评数量


class DoubandetailmoviecommentItem(Item):
    collection='movie'
    movie_comment_url = Field()
    movie_id=Field()#**电影id
    movie_comment_info=Field()#**短评
    movie_commenter_name=Field() #短评者名称
    movie_commenter_id=Field() #id
    movie_commenter_score=Field() #星级
    movie_comment_time=Field() #时间
    movie_comment_useful_number=Field() #有用个数
    movie_comment_content=Field() # 短评内容


class DoubandetailmusicItem(Item):
    collection = 'music'
    music_id = Field()  # 音乐id
    music_url = Field()
    music_alias = Field()  # 别名
    music_name = Field()  # 音乐名字
    music__playbill = Field()  # 海报
    music_performer = Field()  # 表演者
    music_type = Field()  # 流派
    music_album_type=Field()#专辑类型
    music_medium = Field()  # 介质
    music_date = Field()  # 发行日期
    music_publisher = Field()  # 出版者
    music_number_of_records = Field()  # 唱片数
    music_barcode = Field()  # 条形码
    music_other_versions = Field()  #其他版本
    music_star = Field()  # 分数
    music_5score = Field()  # 5星比率
    music_4score = Field()  # 4
    music_3score = Field()  # 3
    music_2score = Field()  # 2
    music_1score = Field()  # 1
    music_describe = Field()  # 简介
    music_comment_number = Field()  # 乐评数量
    #music_review_number = Field()  # 乐评数量

class DoubandetailmusiccommentItem(Item):
    collection = 'music'
    music_comment_url = Field()
    music_id = Field()  # **音乐id
    music_comment_info = Field()  #** 短评
    music_commenter_name = Field()  # 短评者名称
    music_commenter_id = Field()  # id
    music_commenter_score = Field()  # 星级
    music_comment_time = Field()  # 时间
    music_comment_useful_number = Field()  # 有用个数
    music_comment_content = Field()  # 短评内容



class DoubandetailbookItem(Item):
    collection = 'book'
    book_id = Field()  # 书籍id
    book_url = Field()
    book_name = Field()  # 书籍名字
    book_author=Field()#作者
    book_playbill = Field()  # 海报
    book_publisher = Field()  # 出版者
    book_translator=Field() #译者
    book_date = Field()  # 出版年
    book_page_number=Field() # 页数
    book_pricing=Field() #价格
    book_binding=Field() #装帧方式
    book_series=Field()#系列书籍
    book_ISBN = Field()  # ISBN号
    book_star = Field()  # 分数
    book_5score = Field()  # 5星比率
    book_4score = Field()  # 4
    book_3score = Field()  # 3
    book_2score = Field()  # 2
    book_1score = Field()  # 1
    book_describe = Field()  # 简介
    book_comment_number = Field()  # 短评数量
    #book_review_number = Field()  # 书评数量

class DoubandetailbookcommentItem(Item):
    collection = 'book'
    book_id = Field()  # **书籍id
    book_comment_url=Field()
    book_comment_info = Field()  # **短评
    book_commenter_name = Field()  # 短评者名称
    book_commenter_id = Field()  # id
    book_commenter_score = Field()  # 星级
    book_comment_time = Field()  # 时间
    book_comment_useful_number = Field()  # 有用个数
    book_comment_content = Field()  # 短评内容














