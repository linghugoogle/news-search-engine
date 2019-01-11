# -*- coding: utf-8 -*-
"""
Created on Tue Dec 22 16:30:40 2015

@author: bitjoy.net
"""

import jieba
import math
import operator
import sqlite3
import configparser
from datetime import *

class SearchEngine:
    stop_words = set()
    
    config_path = ''
    config_encoding = ''
    
    K1 = 0
    B = 0
    N = 0
    AVG_L = 0
    
    conn = None
    
    def __init__(self, config_path, config_encoding):
        self.config_path = config_path
        self.config_encoding = config_encoding
        config = configparser.ConfigParser()
        config.read(config_path, config_encoding)
        f = open(config['DEFAULT']['stop_words_path'], encoding = config['DEFAULT']['stop_words_encoding'])
        words = f.read()
        self.stop_words = set(words.split('\n'))
        self.conn = sqlite3.connect(config['DEFAULT']['db_path'])
        self.K1 = float(config['DEFAULT']['k1'])
        self.B = float(config['DEFAULT']['b'])
        self.N = int(config['DEFAULT']['n'])
        self.AVG_L = float(config['DEFAULT']['avg_l'])
        

    def __del__(self):
        self.conn.close()
    
    def is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    # 清理列表，与index_module类似
    def clean_list(self, seg_list):
        cleaned_dict = {}
        n = 0
        for i in seg_list:
            i = i.strip().lower()
            if i != '' and not self.is_number(i) and i not in self.stop_words:
                n = n + 1
                if i in cleaned_dict:
                    cleaned_dict[i] = cleaned_dict[i] + 1
                else:
                    cleaned_dict[i] = 1
        return n, cleaned_dict

    # 从数据库中查询关键词
    def fetch_from_db(self, term):
        c = self.conn.cursor()
        c.execute('SELECT * FROM postings WHERE term=?', (term,))
        return(c.fetchone())

    # 构建索引模型，也就是根据输入，输出与之最接近的结果
    def result_by_BM25(self, sentence):

        # 对输入进行分词，获得关键词
        seg_list = jieba.lcut(sentence, cut_all=False)

        # 清理
        n, cleaned_dict = self.clean_list(seg_list)

        # 使用字典保存结果
        BM25_scores = {}

        # 对每个关键词处理，每个关键词的得分累加
        # 计算公式参考：term frequency–inverse document frequency
        # term frequency 词频，一个单词在文档中出现的概率；inverse document frequency 逆文档频率，一个单词在所有文档中出现的概率
        # http://bitjoy.net/2016/01/07/introduction-to-building-a-search-engine-4/
        for term in cleaned_dict.keys():

            # 从数据库中获得关键词对应的文档索引
            r = self.fetch_from_db(term)
            if r is None:
                continue

            # 文档频率，在多少个文档中出现过
            df = r[1]

            # N文档总数，计算IDF，逆文档频率，df越小越好，相应的w越大，这里+0.5避免0，log避免趋近于0
            w = math.log2((self.N - df + 0.5) / (df + 0.5))

            # 文档（docid, date_time, show_number, document_length）
            # 1021	2016-04-28 17:57:00	1	524
            # 1028	2016-04-28 16:53:00	1	487
            docs = r[2].split('\n')
            for doc in docs:
                docid, date_time, tf, ld = doc.split('\t')
                docid = int(docid)
                tf = int(tf)
                ld = int(ld)

                # term frequency
                s = (self.K1 * tf * w) / (tf + self.K1 * (1 - self.B + self.B * ld / self.AVG_L))

                # 每个关键词定的文档的得分进行累加
                if docid in BM25_scores:
                    BM25_scores[docid] = BM25_scores[docid] + s
                else:
                    BM25_scores[docid] = s

        # 按照得分进行排序
        BM25_scores = sorted(BM25_scores.items(), key = operator.itemgetter(1))
        BM25_scores.reverse()

        # 返回结果
        if len(BM25_scores) == 0:
            return 0, []
        else:
            return 1, BM25_scores

    # 按照时间排序
    def result_by_time(self, sentence):
        seg_list = jieba.lcut(sentence, cut_all=False)
        n, cleaned_dict = self.clean_list(seg_list)
        time_scores = {}
        for term in cleaned_dict.keys():
            r = self.fetch_from_db(term)
            if r is None:
                continue
            docs = r[2].split('\n')
            for doc in docs:
                docid, date_time, tf, ld = doc.split('\t')
                if docid in time_scores:
                    continue
                news_datetime = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
                now_datetime = datetime.now()
                td = now_datetime - news_datetime
                docid = int(docid)
                td = (timedelta.total_seconds(td) / 3600) # hour
                time_scores[docid] = td

        # 按照文档出现的时间进行排序
        time_scores = sorted(time_scores.items(), key = operator.itemgetter(1))
        if len(time_scores) == 0:
            return 0, []
        else:
            return 1, time_scores

    # 按照热度排序，将BM25得分和时间组合在一起
    def result_by_hot(self, sentence):
        seg_list = jieba.lcut(sentence, cut_all=False)
        n, cleaned_dict = self.clean_list(seg_list)
        hot_scores = {}
        for term in cleaned_dict.keys():
            r = self.fetch_from_db(term)
            if r is None:
                continue

            # BM25得分中的w
            df = r[1]
            w = math.log2((self.N - df + 0.5) / (df + 0.5))
            docs = r[2].split('\n')
            for doc in docs:
                docid, date_time, tf, ld = doc.split('\t')

                # BM25得分中的得分计算
                docid = int(docid)
                tf = int(tf)
                ld = int(ld)
                BM25_score = (self.K1 * tf * w) / (tf + self.K1 * (1 - self.B + self.B * ld / self.AVG_L))

                # 时间得分
                news_datetime = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
                now_datetime = datetime.now()
                td = now_datetime - news_datetime
                td = (timedelta.total_seconds(td) / 3600) # hour

                # 将BM25得分和时间得分进行组合
                hot_score = math.log(BM25_score) + 1 / td
                if docid in hot_scores:
                    hot_scores[docid] = hot_scores[docid] + hot_score
                else:
                    hot_scores[docid] = hot_score
        # 按照组会得分进行排序
        hot_scores = sorted(hot_scores.items(), key = operator.itemgetter(1))
        hot_scores.reverse()
        if len(hot_scores) == 0:
            return 0, []
        else:
            return 1, hot_scores

    # 按照不同的方式排序
    def search(self, sentence, sort_type = 0):
        if sort_type == 0:
            return self.result_by_BM25(sentence)
        elif sort_type == 1:
            return self.result_by_time(sentence)
        elif sort_type == 2:
            return self.result_by_hot(sentence)

if __name__ == "__main__":
    se = SearchEngine('../config.ini', 'utf-8')
    flag, rs = se.search('北京雾霾', 0)
    print(rs[:10])