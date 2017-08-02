# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import datetime
import re
import scrapy
import redis
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from scrapy.loader import ItemLoader
from w3lib.html import remove_tags
from elasticsearch_dsl.connections import connections
from utils.common_use_func import date_type, get_nums, remove_comment_tags, \
     return_value, join_str, remove_splash, publish_time, get_salary_min, get_salary_max, \
     get_work_years_min, get_work_years_max, get_workaddr, get_longitude, get_latitude
from models.es_types import JobboleType, LianjiaType, LianJia_latitude_longitudeType ,ZhihuQuestionType, ZhihuAnswerType, LaGou
es = connections.create_connection(JobboleType._doc_type.using)
es1 = connections.create_connection(ZhihuQuestionType._doc_type.using)
es2 = connections.create_connection(ZhihuAnswerType._doc_type.using)
es3 = connections.create_connection(LaGou._doc_type.using)
es4 = connections.create_connection(LianjiaType._doc_type.using)
es5 = connections.create_connection(LianJia_latitude_longitudeType._doc_type.using)
redis_cli = redis.StrictRedis(host="localhost")


def gen_suggest(index, info_tuple):
    #根据字符串生成搜索建议数组
    used_word = set()
    suggests = []
    for text, weight in info_tuple:
        if text:
            #调用es的analyze接口分析字符串
            def ES(es_):
                return es_.indices.analyze(index=index, analyzer="ik_max_word",
                                           params={"filter": ["lowercase"]}, body=text)
            if index == JobboleType._doc_type.index:
                words = ES(es)
            elif index == ZhihuQuestionType._doc_type.index:
                words = ES(es1)
            elif index == ZhihuAnswerType._doc_type.index:
                words = ES(es2)
            elif index == LaGou._doc_type.index:
                words = ES(es3)
            elif index == LianjiaType._doc_type.index:
                words = ES(es4)
            else:
                words = ES(es5)
            analyzed_words = set([r["token"] for r in words["tokens"] if len(r["token"]) > 1])
            new_words = analyzed_words - used_word
        else:
            new_words = set()

        if new_words:
            suggests.append({"input": list(new_words), "weight": weight})

    return suggests


class ScrapyspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class JobboleItemLoader(ItemLoader):
    #自定义itemloader
    default_output_processor = TakeFirst()


class JobboleItem(scrapy.Item):
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    title = scrapy.Field()
    create_date = scrapy.Field(
        input_processor=MapCompose(date_type)
    )
    praise_num = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    fav_num = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    comments_num = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    content = scrapy.Field()
    tags = scrapy.Field(
        input_processor=MapCompose(remove_comment_tags),
        output_processor=Join(",")
    )
    front_image_url = scrapy.Field(
        output_processor=MapCompose(return_value)
    )
    front_image_path = scrapy.Field()

    def save_to_es(self):
        # 讲item转换成es的数据
        artcile = JobboleType()
        artcile.title = self["title"]
        artcile.create_date = self["create_date"]
        artcile.content = remove_tags(self["content"])
        artcile.front_image_url = self["front_image_url"]
        if "front_image_path" in self:
            artcile.front_image_path = self["front_image_path"]
        if self["praise_num"]:
            artcile.praise_num = self["praise_num"]
        else:
            artcile.praise_num = 0
        artcile.fav_num = self["fav_num"]
        artcile.comments_num = self["comments_num"]
        artcile.url = self["url"]
        artcile.tags = self["tags"]
        artcile.id = self["url_object_id"]

        artcile.suggest = gen_suggest(JobboleType._doc_type.index, ((artcile.title, 10), (artcile.tags, 7)))

        artcile.save()
        redis_cli.incr("jobbole_count")
        return


class ZhiHuQuestionItem(scrapy.Item):
    zhihu_id = scrapy.Field()
    question_topics = scrapy.Field(
        output_processor=Join(",")
    )
    question_url = scrapy.Field(
        output_processor=Join(",")
    )
    question_title = scrapy.Field(
        output_processor=Join(",")
    )
    question_content = scrapy.Field(
        output_processor=Join(",")
    )
    question_create_date = scrapy.Field()
    question_update_date = scrapy.Field()
    question_answer_num = scrapy.Field(
        input_processor=MapCompose(join_str)
    )
    question_comments_num = scrapy.Field(
        input_processor=MapCompose(join_str)
    )
    question_watch_user_num = scrapy.Field()
    question_follow_num = scrapy.Field()
    question_crawl_time = scrapy.Field()
    question_crawl_updatetime = scrapy.Field()

    def save_to_es(self):
        # 讲item转换成es的数据
        zhihuquestion = ZhihuQuestionType()
        if "zhihu_id" in self:
            zhihuquestion.zhihu_id = self["zhihu_id"][0]
        else:
            zhihuquestion.zhihu_id = 0
        zhihuquestion.topics = ",".join(self.get("topics", ""))
        zhihuquestion.url = self["url"][0]
        zhihuquestion.title = "".join(self["title"])
        zhihuquestion.content = "".join(self["content"])
        zhihuquestion.answer_num = get_nums("".join('%s' % i for i in self.setdefault("answer_num", "no exsits")))
        zhihuquestion.comments_num = get_nums(
            "".join('%s' % i for i in self.setdefault("comments_num", "no exsits")))
        if len(self["watch_user_num"]) == 2:
            zhihuquestion.watch_user_num = int(self["watch_user_num"][0])
            zhihuquestion.click_num = int(self["watch_user_num"][1])
        else:
            zhihuquestion.watch_user_num = int(self["watch_user_num"][0])
            zhihuquestion.click_num = 0
        zhihuquestion.crawl_time = datetime.datetime.now().strftime("%Y-%m-%d")

        zhihuquestion.suggest = gen_suggest(ZhihuQuestionType._doc_type.index,
                                            ((zhihuquestion.title, 10), (zhihuquestion.topics, 7)))

        zhihuquestion.save()
        redis_cli.incr("ZhihuQuestion_count")

        return


class ZhiHuAnswerItem(scrapy.Item):
    zhihu_id = scrapy.Field()
    answer_url = scrapy.Field()
    question_id = scrapy.Field()
    answer_author_id = scrapy.Field()
    answer_content = scrapy.Field()
    answer_praise_num = scrapy.Field()
    answer_comments_num = scrapy.Field()
    answer_create_date = scrapy.Field()
    answer_update_date = scrapy.Field()
    crawl_time = scrapy.Field()
    answer_crawl_updatetime = scrapy.Field()

    def save_to_es(self):
        # 讲item转换成es的数据
        create_time = datetime.datetime.fromtimestamp(self["create_time"]).strftime("%Y-%m-%d")
        update_time = datetime.datetime.fromtimestamp(self["update_time"]).strftime("%Y-%m-%d")
        zhihuanswer = ZhihuAnswerType()
        zhihuanswer.zhihu_id = self["zhihu_id"]
        zhihuanswer.question_id = self["question_id"]
        zhihuanswer.author_id = self["author_id"]
        zhihuanswer.content = self["content"]
        zhihuanswer.praise_num = self["praise_num"]
        zhihuanswer.comments_num = self["comments_num"]
        zhihuanswer.create_time = create_time
        zhihuanswer.update_time = update_time
        zhihuanswer.crawl_time = self["crawl_time"].strftime("%Y-%m-%d")

        zhihuanswer.suggest = gen_suggest(ZhihuAnswerType._doc_type.index,
                                          ((zhihuanswer.content, 10),))

        zhihuanswer.save()
        redis_cli.incr("ZhihuAnswer_count")
        return


class LaGouItemLoader(ItemLoader):
    #自定义itemloader
    default_output_processor = TakeFirst()


class LaGouJobItem(scrapy.Item):
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    title = scrapy.Field()
    salary_min = scrapy.Field(
        input_processor=MapCompose(get_salary_min)
    )
    salary_max = scrapy.Field(
        input_processor=MapCompose(get_salary_max)
    )
    company_name = scrapy.Field()
    job_city = scrapy.Field(
        input_processor=MapCompose(remove_splash)
    )
    work_years_min = scrapy.Field(
        input_processor=MapCompose(get_work_years_min)
    )
    work_years_max = scrapy.Field(
        input_processor=MapCompose(get_work_years_max)
    )
    education_degree = scrapy.Field(
        input_processor=MapCompose(remove_splash)
    )
    job_type = scrapy.Field()
    publish_time = scrapy.Field(
        input_processor=MapCompose(publish_time)
    )
    tags = scrapy.Field()
    job_advantage = scrapy.Field()
    job_desc = scrapy.Field()
    job_addr = scrapy.Field(
        input_processor=MapCompose(remove_tags, get_workaddr)
    )
    company_url = scrapy.Field()
    crwal_time = scrapy.Field()
    crwal_update_time = scrapy.Field()

    def save_to_es(self):
        # 讲item转换成es的数据
        lagoujob = LaGou()
        lagoujob.url = self["url"]
        lagoujob.url_object_id = self["url_object_id"]
        lagoujob.title = self.setdefault("title", "not exist")
        lagoujob.salary = self["salary"]
        lagoujob.job_city = self["job_city"]
        lagoujob.work_years = self["work_years"]
        lagoujob.degree_need = self["degree_need"]
        lagoujob.job_type = self["job_type"]
        lagoujob.publish_time = self["publish_time"]
        lagoujob.tags = self.setdefault("tags", "not exist")
        lagoujob.job_advantage = self["job_advantage"]
        lagoujob.job_desc = self["job_desc"]
        lagoujob.job_addr = self["job_addr"]
        lagoujob.company_url = self["company_url"]
        lagoujob.company_name = self["company_name"]
        lagoujob.crawl_time = self["crawl_time"].strftime("%Y-%m-%d")

        lagoujob.suggest = gen_suggest(LaGou._doc_type.index,
                                       ((lagoujob.title, 10), (lagoujob.job_city, 7),
                                        (lagoujob.job_advantage, 5), (lagoujob.degree_need, 4)))

        lagoujob.save()
        redis_cli.incr("lagoujob_count")

        return


class LianJiaItemLoader(ItemLoader):
    #自定义itemloader
    default_output_processor = TakeFirst()


class LianJiaItem(scrapy.Item):
    url = scrapy.Field()
    lianjia_id = scrapy.Field()
    residential_district_name = scrapy.Field()
    residential_district_url = scrapy.Field()
    title = scrapy.Field()
    region = scrapy.Field()
    region_detail = scrapy.Field()
    address = scrapy.Field()
    house_area = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    room_count = scrapy.Field()
    face_direction = scrapy.Field()
    rent_price = scrapy.Field()
    floor = scrapy.Field()
    publish_time = scrapy.Field()
    total_watch_count = scrapy.Field(
        input_processor=MapCompose(get_nums)
    )
    crwal_time = scrapy.Field()
    crwal_update_time = scrapy.Field()

    def save_to_es(self):
        # 讲item转换成es的数据
        lianjia_house = LianjiaType()
        lianjia_house.url = self["url"][0]
        lianjia_house.lianjia_id = self["lianjia_id"][0]
        lianjia_house.residential_district_name = ""
        if 'shz' in self["url"][0]:
            lianjia_house.residential_district_name = self["residential_district_name"][0]
        elif 'shzr' in self["url"][0]:
            lianjia_house.residential_district_name = self["residential_district_name"][0]
        lianjia_house.residential_district_url = self["residential_district_url"]
        lianjia_house.title = self["title"][0]
        lianjia_house.region = self["region"][0]
        lianjia_house.region_detail = self["region"][1]
        lianjia_house.address = self["address"][0]
        lianjia_house.house_area = float(self["house_area"][0]) * 1.0
        lianjia_house.room_count = int(self["room_count"][0][0])
        lianjia_house.face_direction = "".join(self["face_direction"]).strip()
        lianjia_house.rent_price = int("".join(self["rent_price"]))
        lianjia_house.floor = self["floor"][0]
        lianjia_house.publish_time = self["publish_time"][0]
        try:
            lianjia_house.total_watch_count = self["total_watch_count"][0]
        except KeyError:
            lianjia_house.total_watch_count = 0
        lianjia_house.crwal_time = datetime.datetime.now()

        lianjia_house.suggest = gen_suggest(LianjiaType._doc_type.index,
                                            ((lianjia_house.title, 10), (lianjia_house.region_detail, 7)))

        lianjia_house.save()
        redis_cli.incr("lianjia_house_count")
        return


class LianJia_latitude_longitude(scrapy.Item):
    residential_district_name = scrapy.Field()
    residential_district_url = scrapy.Field()
    lianjia_id = scrapy.Field()
    longitude = scrapy.Field(
        input_processor=MapCompose(get_longitude)
    )
    latitude = scrapy.Field(
        input_processor=MapCompose(get_latitude)
    )
    crwal_time = scrapy.Field()
    crwal_update_time = scrapy.Field()

    def save_to_es(self):
        # 讲item转换成es的数据
        LianJia_latitude_longitude = LianJia_latitude_longitudeType()
        LianJia_latitude_longitude.residential_district_name = self["residential_district_name"]
        LianJia_latitude_longitude.residential_district_url = self["residential_district_url"]
        LianJia_latitude_longitude.lianjia_id = self["lianjia_id"]
        LianJia_latitude_longitude.longitude = self["longitude"][0]
        LianJia_latitude_longitude.latitude = self["latitude"][0]
        LianJia_latitude_longitude.crwal_time = datetime.datetime.now()
        LianJia_latitude_longitude.crwal_update_time = datetime.datetime.now()

        LianJia_latitude_longitude.suggest = gen_suggest(
            LianjiaType._doc_type.index, ((LianJia_latitude_longitude.residential_district_name, 10),))

        LianJia_latitude_longitude.save()
        redis_cli.incr("LianJia_latitude_longitude_count")
        return
