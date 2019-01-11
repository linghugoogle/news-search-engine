# -*- coding: utf-8 -*-
"""
Created on Sat Dec  5 23:31:22 2015

@author: bitjoy.net
"""

from os import listdir
import xml.etree.ElementTree as ET
import jieba
import sqlite3
import configparser

class Doc:
    docid = 0
    date_time = ''
    tf = 0
    ld = 0
    # id，时间，词频，文档长度
    def __init__(self, docid, date_time, tf, ld):
        self.docid = docid
        self.date_time = date_time
        self.tf = tf
        self.ld = ld
    def __repr__(self):
        return(str(self.docid) + '\t' + self.date_time + '\t' + str(self.tf) + '\t' + str(self.ld))
    def __str__(self):
        return(str(self.docid) + '\t' + self.date_time + '\t' + str(self.tf) + '\t' + str(self.ld))

class IndexModule:
    stop_words = set()
    postings_lists = {}
    
    config_path = ''
    config_encoding = ''
    
    def __init__(self, config_path, config_encoding):
        self.config_path = config_path
        self.config_encoding = config_encoding
        config = configparser.ConfigParser()
        config.read(config_path, config_encoding)
        f = open(config['DEFAULT']['stop_words_path'], encoding = config['DEFAULT']['stop_words_encoding'])
        words = f.read()
        self.stop_words = set(words.split('\n'))

    # 判断是否是数字
    def is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False

    # 对每个文档的分词结果进行清洗
    def clean_list(self, seg_list):

        # 使用字典进行存储
        cleaned_dict = {}

        # 单词总数
        n = 0

        #每个词语处理，i为词语
        for i in seg_list:
            i = i.strip().lower()

            # 不为空、不为数字、不为停止词
            if i != '' and not self.is_number(i) and i not in self.stop_words:
                n = n + 1
                if i in cleaned_dict:
                    # 字典记录单词和对应的词频
                    cleaned_dict[i] = cleaned_dict[i] + 1
                else:
                    cleaned_dict[i] = 1

        # 返回词语总数和词语出现次数
        return n, cleaned_dict

    # 保存每一条记录到sqlite数据库
    def write_postings_to_db(self, db_path):
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        c.execute('''DROP TABLE IF EXISTS postings''')
        c.execute('''CREATE TABLE postings
                     (term TEXT PRIMARY KEY, df INTEGER, docs TEXT)''')

        # {word: (document_frequecy, (docid, date_time, show_number, document_length))}
        # key为词语, value每个词语对应的位置信息
        for key, value in self.postings_lists.items():

            # value[0] 为document_frequency，某词项在不同文档中出现的次数
            # value[1]为倒排索引位置信息
            doc_list = '\n'.join(map(str,value[1]))

            t = (key, value[0], doc_list)
            c.execute("INSERT INTO postings VALUES (?, ?, ?)", t)

        conn.commit()
        conn.close()
    
    def construct_postings_lists(self):
        config = configparser.ConfigParser()
        config.read(self.config_path, self.config_encoding)
        files = listdir(config['DEFAULT']['doc_dir_path'])

        # 所有文档平均长度
        AVG_L = 0

        # 遍历每一个XML文档
        for i in files:

            # 使用ElementTree获得每个文档的信息
            root = ET.parse(config['DEFAULT']['doc_dir_path'] + i).getroot()
            title = root.find('title').text
            body = root.find('body').text
            docid = int(root.find('id').text)
            date_time = root.find('datetime').text

            # 使用结巴分词对标题和正文进行分词
            seg_list = jieba.lcut(title + '。' + body, cut_all=False)

            # 对分词进行清洗，ld 为文档单词总数，cleaned_dict为字典，每个词语和对应的次数
            ld, cleaned_dict = self.clean_list(seg_list)

            # 对每个文档处理之后，更新所有文档总长度
            AVG_L = AVG_L + ld

            # 遍历cleaned_dict字典，key为词语，value为出现次数
            for key, value in cleaned_dict.items():
                # 每一条记录
                d = Doc(docid, date_time, value, ld)

                # postings_lists为字典内嵌元组,元组内嵌元组，key为词语，{word:(document_frequecy,(docid, date_time, show_number, document_length))}
                if key in self.postings_lists:
                    self.postings_lists[key][0] = self.postings_lists[key][0] + 1 # df++
                    self.postings_lists[key][1].append(d)
                else:
                    self.postings_lists[key] = [1, [d]] # [df, [Doc]]

        # 获得平均每个文档的长度
        AVG_L = AVG_L / len(files)
        config.set('DEFAULT', 'N', str(len(files)))
        config.set('DEFAULT', 'avg_l', str(AVG_L))

        # 保存文档数量、平均长度信息到config.ini
        with open(self.config_path, 'w', encoding = self.config_encoding) as configfile:
            config.write(configfile)

        # 保存每一条记录到sqlite数据库
        self.write_postings_to_db(config['DEFAULT']['db_path'])

if __name__ == "__main__":
    # 设置默认参数
    im = IndexModule('../config.ini', 'utf-8')

    # 构建倒排索引数据库
    im.construct_postings_lists()
