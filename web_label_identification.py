#coding=utf8
"""
description: Identify the tags of a web page
date: 2019_04_12
url：http://oei.hust.edu.cn/szdw.htm 华中科技大学 光电
aurhor：xiajuntao
"""
import sys
import os
import re
from bs4 import BeautifulSoup
import logging
import pymongo
import base64
# from goose3 import Goose
# from goose3.text import StopWordsChinese
import urllib
import time, requests
import datetime, random
from etl.utils.log_conf import configure_logging
import traceback
from etl.data_gather.settings import SAVE_MONGO_CONFIG, RESOURCE_DIR
from etl.common_spider.donwloader import Downloader
import chardet
import pandas as pd
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import jieba as jb
import re
from collections import Counter
# from wordcloud import WordCloud
# from sklearn.feature_extraction.text import TfidfVectorizer
# from sklearn.feature_selection import chi2
# from sklearn.model_selection import train_test_split
# from sklearn.feature_extraction.text import CountVectorizer
# from sklearn.feature_extraction.text import TfidfTransformer
# from sklearn.naive_bayes import MultinomialNB
# from sklearn.metrics import f1_score
# from sklearn.metrics import confusion_matrix
# from sklearn.metrics import classification_report
import chardet
import urllib.request
# from boilerpipe.extract import Extractor
import html2text
# import pyhanlp
from collections import OrderedDict


class ListDetailSpider(object):
    def __init__(self, config, proj=None):
        config["db"] = 'lz_data_yewu'
        self.proj = proj
        self.host = "oei.hust.edu.cn"  # 网站域名
        self.host_name = "华中科技大学"  # 网站中文名
        self.api_url = "http://oei.hust.edu.cn/szdw.htm"  # 起始URL或者是基础URL，请求的链接在此基础生成
        self.mongo_client = self.get_mongo(**config)
        self.save_coll_name = "university_teacher_gd_huazhongkejidaxue"  # 需要保存的表名
        self.mongo_db = self.mongo_client[config["db"]]
        self.mongo_coll = self.mongo_db[self.save_coll_name]
        self.start_down_time = datetime.datetime.now()
        self.down_retry = 3
        configure_logging("HZKJ_GD.log")  # 日志文件名
        self.logger = logging.getLogger("spider")
        self.downloader = Downloader(self.logger, need_proxy=False)  # 注意是否需要使用代理更改参数
        self.headers = {
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
            'Referer': '',
            'Host': self.host,
        }
        self.headers2 = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:68.0) Gecko/20100101 Firefox/68.0",
        }
        # 链接mongodb
        # self.g = Goose({'stopwords_class': StopWordsChinese})
        self.teacher_title = ["博士生导师", "硕士生导师", "教授", "副教授", "博导", "硕导", "讲师", "工程师"]
        self.research_field = ["研究方向","研究兴趣","研究领域"]
        self.award = ["获得奖励", "获奖", "荣誉","奖励"]
        self.experience = ["教育背景", "教育经历", "经历", "工作经历", "学习经历", "学习"]
        self.article = ["论文", "期刊",]
        self.resume = ["个人简介", "简介", "介绍", "成果"]
        self.patent = ["专利",]
        self.book = ["著作", "书", "教材", "专著", "论著"]
        self.project = ["项目", "课题", "在研"]
        self.social_appointments = ["兼职", "社会兼职", "任职", "审稿人", "评审"]
        self.others = ["招生", "招聘", "会议", "报告", "课程", "指导", "研究生",]
        self.achievements = ["成果"]
        # self.research_field = ["", "", "", "", "", "", "", "", "", "", "", "", "", ]


    def get_mongo(self, host, port, db, username, password):
        if username and password:
            url = "mongodb://%s:%s@%s:%s/%s" % (username, password, host, port, db)
        else:
            url = "mongodb://%s:%s" % (host, port)
        return pymongo.MongoClient(url)


    def save_record(self, record, coll_name, pk):
        my_coll = self.mongo_db[coll_name]
        tmp = []

        for k, v in pk.items():
            tmp.append("%s=%s" % (k, v))
            # print( tmp)
        show = "  ".join(tmp)
        r_in_db = my_coll.find_one(pk)
        print ('show：' + show)
        if not r_in_db:
            my_coll.insert_one(record)
            self.logger.info("成功插入(%s)  %s" % (record['teacher_name'], show))




    def run(self, start_page=1, max_page=-1):
        """
        数据采集主入口
        :return:
        """
        self.logger.info("Begin Run")
        # ============主页面获取==============================
        page_no = start_page
        total_page = max_page   # 传入的最大页数参数
        page_current = page_no


        # department = None
        # try:

        cur_url = "%s" % (self.api_url)  # 拼接当前请求的URL
        print(cur_url)
        resp = self.downloader.crawl_data(cur_url, None, self.headers, "get")  # 使用downloader文件的ip代理请求
        if resp:
            resp.encoding = 'utf-8'
            content = resp.text
            soup = BeautifulSoup(content, "lxml")  # 进入每一个分页
            # print( soup)


            self.logger.info("页数(%s)" % (page_current))
            c = 0  # c表示每两个class换一个学院部
            tag = soup.find('div', {'class':'nr'})
            # print(tag)
            tag1 = tag.find_all('li')
            for tag1_1 in tag1:
                c += 1
                img_url = ''
                tag2 = tag1_1.find('a')['href'].replace('..', '')
                # print(tag2)
                print( c)
                teacher_name = tag1_1.find('a').get_text().strip().replace('姓名：', '')
                # print( teacher_name)
                if 'http' not in str(tag2):
                    detail_url = 'http://oei.hust.edu.cn/'+tag2
                    # print( detail_url)
                else:
                    detail_url = 'http://oei.hust.edu.cn/'+tag2
                    # print(detail_url)
                pk = {"url": detail_url}
                # r_in_db = self.mongo_coll.find_one(pk)
                # tag5 = tag1_1.find('img')['src']
                # if tag5:
                #     tag6 = 'http://www.aiar.xjtu.edu.cn' + tag5
                #     img_url = tag6
                if detail_url == 'http://www.cnel.ufl.edu/people/people.php?name=principe':
                    continue

                # if not r_in_db:
                    # try:
                detail_url = 'http://www.orihard.com/product/82.htm'

                detail_resp = self.downloader.crawl_data(detail_url, None, self.headers2,"get")  # 使用downloader文件的ip代理请求
                TestData = urllib.request.urlopen(detail_url).read()
                bianma = chardet.detect(TestData)
                print("编码-----------: {} \t detail_url: {} \t ".format(bianma, detail_url))
                print(bianma['encoding'])
                detail_resp.encoding = bianma['encoding']
                detail_content = detail_resp.text
                # print(detail_content)
                record = self.parse_detail(detail_url, teacher_name, detail_content,img_url)


                # if record:
                #     self.save_record(record, self.save_coll_name, {"url": record["url"]})
                    # except Exception as e:
                    #     print(e)
                # else:
                #     self.logger.info("重复的数据(%s)" % (teacher_name) ) # 重复数据打印到日志



            else:
                print( '不存在抓取列表，跳过')

        # except:
        #     self.logger.error("页面抓取出错，页数(%s) error:%s" % (page_current, traceback.format_exc()))

        self.logger.info("Finish Run")

    # 定义删除除汉字以外的所有符号的函数
    def remove_punctuation(self, line):
        line = str(line)
        if line.strip() == '':
            return ''
        rule = re.compile(u"[^*\u4E00-\u9FA5]")
        line = rule.sub(' ', line)
        line = line.replace(' ', '')
        return line

    def stopwordslist(self, filepath):
        stopwords = [line.strip() for line in open(filepath, 'r', encoding='utf-8').readlines()]
        return stopwords

    def key_word_collection_and_judge_branch(self, classification_list_each, title_judge_dict, name, format_sec, num):
        for i in classification_list_each:
            if i in str(format_sec):
                classification = name
                if (classification in title_judge_dict.keys()):
                    return classification
                else:
                    classification = classification + str(num)
                    return classification

    # 各个分类的关键词判断啊
    def key_word_collection_and_judge(self, title_judge, num, title_judge_dict):
        stopwords = self.stopwordslist("D:\Python\python_code\Liangzhi\TianPengTrans-tmp\etl\pytorch\百度停用词表_修改.txt")
        delete_word1 = re.findall(r"\((.+?)\)", title_judge)
        delete_word2 = re.findall(r"（(.+?)）", title_judge)
        for each_1 in delete_word1:
            title_judge = title_judge.replace(each_1, '')
        for each_2 in delete_word2:
            title_judge = title_judge.replace(each_2, '')
        format_sec = "".join([w for w in list((self.remove_punctuation(title_judge.replace('*', '').replace('#', '')))) if w not in stopwords])
        print(format_sec)

        if len(format_sec) < 10:
            classification = self.key_word_collection_and_judge_branch(self.research_field, title_judge_dict, 'research_field', format_sec, num)
            if classification:
                return classification, num
            classification = self.key_word_collection_and_judge_branch(self.award, title_judge_dict, 'award', format_sec, num)
            if classification:
                return classification, num
            classification = self.key_word_collection_and_judge_branch(self.experience, title_judge_dict, 'experience', format_sec, num)
            if classification:
                return classification, num
            classification = self.key_word_collection_and_judge_branch(self.article, title_judge_dict, 'journal_article', format_sec, num)
            if classification:
                return classification, num
            classification = self.key_word_collection_and_judge_branch(self.patent, title_judge_dict, 'patent',format_sec, num)
            if classification:
                return classification, num
            classification = self.key_word_collection_and_judge_branch(self.book, title_judge_dict, 'book', format_sec, num)
            if classification:
                return classification, num
            classification = self.key_word_collection_and_judge_branch(self.social_appointments, title_judge_dict, 'social_appointments', format_sec, num)
            if classification:
                return classification, num
            classification = self.key_word_collection_and_judge_branch(self.project, title_judge_dict, 'project', format_sec,num)
            if classification:
                return classification, num
            classification = self.key_word_collection_and_judge_branch(self.achievements, title_judge_dict, 'achievements', format_sec, num)
            if classification:
                return classification, num

        others_return = 'others'+str(num)
        return others_return, num

    def add_classification_list(self, title_judge_dict, content_list, records):


        flag = 0
        for key, value in title_judge_dict.items():
            if flag == 0:
                if "others" in str(key):
                    pass
                else:
                    key_first = key
                    value_first = value
                    flag = 1
            elif flag == 1:
                if "others" in key_first:
                    pass
                else:
                    rule = re.compile(u"[^_A-Za-z]")        # 这两行是拼接相同栏目的数据
                    key_first = rule.sub('', key_first)    # 这两行是拼接相同栏目的数据

                    add_each_list = content_list[int(value_first+1):int(value)]
                    for each in add_each_list:
                        each = each.replace('*', '').replace('#', '')
                        print(each)
                        if each == '':
                            pass
                        else:
                            if 'achievements' in str(key_first):
                                records['others'].append(each)
                            else:
                                records[key_first].append(each)
                    if "others" in str(key):
                        flag = 0

                print(records)
                # print(records[key_first])
                print('------------------------------')
                key_first = key
                value_first = value
        return records

    def parse_detail(self, detail_url, teacher_name,  detail_content, img_url):
        record = {}
        record["university"] = self.host_name
        record["college"] = '光学科学与工程学院'
        record["resume"] = ''
        record["laboratory"] = ''
        record["category"] = '光电产业'
        record["native_place"] = ''
        record["source"] = '华中科技大学'
        record["html"] = detail_content

        resume = ''
        book = []
        email = ''
        phone = ''
        job_title = ''
        professional_title = ''
        department = ''
        content = ''
        content_list = []
        title_judge_dict = {}
        research_field = []
        education = ''
        patent = []
        award = []
        social_appointments = []
        others = []
        experience = []
        journal_article = []
        project = []


        record["project"] = project
        record["job_title"] = job_title
        record["book"] = book
        record["resume"] = resume
        record["research_field"] = research_field
        record["professional_title"] = professional_title
        record["patent"] = patent
        record["award"] = award
        record["social_appointments"] = social_appointments
        record["others"] = others
        record["experience"] = experience
        record["journal_article"] = journal_article


        img_url = ''
        soup = BeautifulSoup(detail_content,'lxml')

        # article = self.g.extract(url=detail_url)
        # print(type(article))
        # your_url = 'http://gr.xjtu.edu.cn/web/meikuizhi'
        # extractor = Extractor(extractor='ArticleExtractor', url=your_url)
        # extractor = Extractor(extractor='KeepEverythingExtractor', url=detail_url)
        # extracted_text = extractor.getText()

        # print(extracted_text2)
        # content_all = extracted_text
        # print("extracted_text: {} \t detail_url: {} \t ".format(content_all, detail_url))
        # extractor2 = Extractor(extractor='ArticleExtractor', url=detail_url)
        # extracted_text2 = extractor2.getText()
        # resume = extracted_text2.replace('【来源：信息科学与工程学院   |  发布日期：2019-05-05  】     【选择字号： 大 中 小 】','').replace('【来源：信息科学与工程学院   |  发布日期：2018-01-26  】     【选择字号： 大 中 小 】', '')

        o = 0
        content_tag = soup.find('tbody')

        if content_tag:
            # 去除属性script
            [s.extract() for s in content_tag("script")]
            # content_tag = content_tag.get_text().strip().replace('暂无内容', '')

            h = html2text.HTML2Text()
            h.ignore_links = True

            # print(html2text.HTML2Text((str(content_tag))))

            content_tag1 = h.handle(str(content_tag)).replace('Ø', '').replace('ü', '').replace('§' ,'')
            # print(content_tag1)

            content1 = str(content_tag1).split('\n')

            for each in content1:
                if len(each) > 1:
                    if each.startswith("!") and each.endswith(")"):
                        pass
                    else:
                        each = each.strip()
                        print(each)
                        content += each.replace('*', '').replace('#', '') + '\n'
                        content_list.append(each)


            delete1 = re.findall('!\[(.+?).jpg\)', str(content))
            if delete1:
                delete1 = delete1[0]
                content = content.strip('\n').replace(delete1, '').replace('![', '').replace('.jpg)', '')
            else:
                content = content.strip('\n')
            # print(content)

            num = 0
            if content_list:
                for title_judge in content_list:
                    if (title_judge.startswith("**") and title_judge.endswith("**")) or title_judge.startswith('###') or title_judge.startswith('####'):
                        if title_judge == "****" or title_judge == '###' or title_judge == '####':
                            pass
                        else:
                            classification, num = self.key_word_collection_and_judge(title_judge, num, title_judge_dict)
                            print('num:', num)
                            num += 1
                            print("class：%s---sentence: %s" % (classification, title_judge))
                            list_index_number = content_list.index(title_judge)
                            # for key, value in title_judge_dict.items():
                            #     if key == classification:
                            #         classification = classification + str(num)
                            # if title_judge_dict[classification] == None:
                            #     list_number_classification = []
                            #
                            #     list_index_number.append(list_index_number)
                            #     title_judge_dict[classification] = list_index_number
                            # else:
                            #
                            #     title_judge_dict[classification] =
                            title_judge_dict[classification] = list_index_number   # 字典啊啊啊啊啊啊
                            print(title_judge_dict)

            list_research_field = []
            list_award = []
            list_experience = []
            list_article = []
            # list_resume = []
            list_patent = []
            list_book = []
            list_project = []
            list_social_appointments = []
            list_others = []

            records = {
                'research_field': list_research_field,
                'award': list_award,
                'experience': list_experience,
                'journal_article': list_article,
                # 'resume': list_resume,
                'patent': list_patent,
                'book': list_book,
                'project': list_project,
                'social_appointments': list_social_appointments,
                'others': list_others,
            }


            if title_judge_dict:
                if content_list:    # 判断是否需要判断结尾在第几行，就是最后一个分类是不是需要的分类，是就需要判断，不是就不需要判断
                    order_of_keys = title_judge_dict.keys()
                    list_of_tuples = [(key, title_judge_dict[key]) for key in order_of_keys]
                    your_dict = OrderedDict(list_of_tuples)
                    last_one = next(reversed(your_dict))
                    print(next(reversed(your_dict)))
                    length = len(content_list)
                    if "others" not in str(last_one):
                        title_judge_dict['last_line'] = length

                    records = self.add_classification_list(title_judge_dict, content_list, records)
                    record.update(records)




            pat3 = '(\w)+(\.\w+)*@(\w)+((\.\w+)+)'

            matched_address = re.search(pat3, content)
            if matched_address:
                email = matched_address.group()

            m = re.findall(r'\(?0\d{2,3}[)-]?\d{7,8}', content)
            if m:  # 这里只取一个电话号码 因为正则匹配出来的优惠很多多余的
                phone = m[0]
                # 这里是抓取所有电话号码的代码
                # for m_each in m:
                #     if len(m) == 1:
                #         phone = m_each
                #     else:
                #         phone += m_each + ','


            for title_each in self.teacher_title:
                if title_each in str(content):
                    if '博士生导师' in record['professional_title']:
                        if title_each == '博导':
                            continue
                    if '硕士生导师' in record['professional_title']:
                        if title_each == '硕导':
                            continue
                    if '教授' in record['professional_title']:
                        if title_each == '副教授':
                            continue

                    if not o == 1:
                        record["professional_title"] += title_each
                        o = 1
                    else:
                        record["professional_title"] += ',' + title_each

            if '博士生导师' in record['professional_title'] or '硕士生导师' in record['professional_title'] or '博士' in str(content):
                education = '博士'

            try:
                img_url_tag = content_tag.find('img')['src']
                if img_url_tag:
                    img_url = 'http://oei.hust.edu.cn'+img_url_tag
            except Exception as e:
                print(e)

        record["education"] = education
        record["department"] = department
        record["email"] = email
        record["phone"] = phone
        record["resume"] = content
        record["url"] = detail_url
        record["teacher_name"] = teacher_name
        record["img_url"] = img_url

        return record


if __name__ == '__main__':
    bp = ListDetailSpider(SAVE_MONGO_CONFIG)
    bp.run(start_page=1, max_page=2)