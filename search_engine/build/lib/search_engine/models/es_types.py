from datetime import datetime
from elasticsearch_dsl import DocType, Date, Nested, Boolean, \
    analyzer, InnerObjectWrapper, Completion, Keyword, Text, Integer
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer
connections.create_connection(hosts=["localhost"])


class CustomAnalyzer(_CustomAnalyzer):

    def get_analysis_definition(self):
        return {}

ik_analyzer = CustomAnalyzer("ik_max_word", filter=["lowercase"])


class JobboleType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    url = Keyword()
    url_object_id = Keyword()
    title = Text(analyzer="ik_max_word")
    create_date = Date()
    praise_num = Integer()
    fav_num = Integer()
    comments_num = Integer()
    content = Text(analyzer="ik_max_word")
    tags = Text(analyzer="ik_max_word")
    front_image_url = Keyword()

    class Meta:
        index = "jobbole"
        doc_type = "article"


class ZhihuQuestionType(DocType):
    # 伯乐在线文章类型
    suggest = Completion(analyzer=ik_analyzer)
    zhihu_id = Keyword()
    topics = Text(analyzer="ik_max_word")
    url = Keyword()
    title = Text(analyzer="ik_max_word")
    content = Text(analyzer="ik_max_word")
    answer_num = Integer()
    comments_num = Integer()
    watch_user_num = Integer()
    click_num = Integer()
    crawl_time = Date()

    class Meta:
        index = "zhihu"
        doc_type = "zhihuquestion"


class ZhihuAnswerType(DocType):
    # 伯乐在线文章类型
    suggest = Completion(analyzer=ik_analyzer)
    zhihu_id = Keyword()
    url = Keyword()
    question_id = Keyword()
    author_id = Keyword()
    content = Text(analyzer="ik_max_word")
    praise_num = Integer()
    comments_num = Integer()
    create_time = Date()
    update_time = Date()
    crawl_time = Date()

    class Meta:
        index = "zhihu"
        doc_type = "zhihuanswer"


class LaGou(DocType):
    # 伯乐在线文章类型
    suggest = Completion(analyzer=ik_analyzer)
    url = Keyword()
    url_object_id = Keyword()
    title = Text(analyzer="ik_max_word")
    salary = Text(analyzer="ik_max_word")
    job_city = Text(analyzer="ik_max_word")
    work_years = Text(analyzer="ik_max_word")
    degree_need = Text(analyzer="ik_max_word")
    job_type = Text(analyzer="ik_max_word")
    publish_time = Date()
    tags = Text(analyzer="ik_max_word")
    job_advantage = Text(analyzer="ik_max_word")
    job_desc = Text(analyzer="ik_max_word")
    job_addr = Text(analyzer="ik_max_word")
    company_url = Keyword()
    company_name = Text(analyzer="ik_max_word")
    crawl_time = Date()

    class Meta:
        index = "lagou"
        doc_type = "lagoujob"


class LianjiaType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    url = Keyword()
    lianjia_id = Keyword()
    residential_district_name = Text(analyzer="ik_max_word")
    residential_district_url = Keyword()
    title = Text(analyzer="ik_max_word")
    region = Text(analyzer="ik_max_word")
    region_detail = Text(analyzer="ik_max_word")
    address = Text(analyzer="ik_max_word")
    house_area = Integer()
    room_count = Integer()
    face_direction = Text(analyzer="ik_max_word")
    rent_price = Integer()
    floor = Text(analyzer="ik_max_word")
    publish_time = Date()
    total_watch_count = Integer()
    crwal_time = Date()

    class Meta:
        index = "lianjia"
        doc_type = "lianjia_house"


class LianJia_latitude_longitudeType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    residential_district_name = Text(analyzer="ik_max_word")
    residential_district_url = Keyword()
    lianjia_id = Keyword()
    longitude = Integer()
    latitude = Integer()
    crwal_time = Date()

    class Meta:
        index = "lianjia"
        doc_type = "lianjia_latitude_longitude"

if __name__ == "__main__":
    JobboleType.init()
    LianjiaType.init()
    LianJia_latitude_longitudeType.init()