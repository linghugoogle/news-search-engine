# -*- coding: utf-8 -*-
"""
Created on Sat Dec 19 11:57:01 2015

@author: bitjoy.net
"""

from bs4 import BeautifulSoup
import urllib.request
import xml.etree.ElementTree as ET
import configparser

# 获得所有新闻页面
def get_news_pool(root, start, end):

    # 使用数组存储
    news_pool = []

    # 对每个页面进行处理
    for i in range(start,end,-1):

        # 获得每个页面的URL
        page_url = ''
        if i != start:
            page_url = root +'_%d.shtml'%(i)
        else:
            page_url = root + '.shtml'
        try:
            response = urllib.request.urlopen(page_url)
        except Exception as e:
            print("-----%s: %s-----"%(type(e), page_url))
            continue
        print(page_url)

        # 读取URL并使用bs处理
        html = response.read()
        soup = BeautifulSoup(html,"lxml") # http://www.crummy.com/software/BeautifulSoup/bs4/doc.zh/
        td = soup.find('td', class_ = "newsblue1")
        a = td.find_all('a')
        span = td.find_all('span')

        # 对bs找到的每一个<a></a>进行处理，获得时间、URL和标题
        for i in range(len(a)):
            date_time = span[i].string
            url = a[i].get('href')
            title = a[i].string
            news_info = ['2016-'+date_time[1:3]+'-'+date_time[4:-1]+':00',url,title]
            news_pool.append(news_info)
    # 返回二维数组
    return(news_pool)

# 抓取每个新闻页面信息
def crawl_news(news_pool, min_body_len, doc_dir_path, doc_encoding):
    i = 1

    # 遍历每一个新闻URL，news_pool为二维数组，news为一位数组
    for news in news_pool:

        # 访问每一个新闻URL
        try:
            response = urllib.request.urlopen(news[1])
        except Exception as e:
            print("-----%s: %s-----"%(type(e), news[1]))
            continue
        print("News:",news[1])

        # 读取每个URL并使用bs处理
        html = response.read()
        soup = BeautifulSoup(html,"lxml") # http://www.crummy.com/software/BeautifulSoup/bs4/doc.zh/
        try:
            body = soup.find('div', class_ = "text clear").find('div').get_text()
        except Exception as e:
            print("-----%s: %s-----"%(type(e), news[1]))
            continue

        # 去除//
        if '//' in body:
            body = body[:body.index('//')]
        body = body.replace(" ", "")

        # 正文长度果断，去掉
        if len(body) <= min_body_len:
            continue

        # 保存为XML结构化文本
        doc = ET.Element("doc")
        ET.SubElement(doc, "id").text = "%d"%(i)
        ET.SubElement(doc, "url").text = news[1]
        ET.SubElement(doc, "title").text = news[2]
        ET.SubElement(doc, "datetime").text = news[0]
        ET.SubElement(doc, "body").text = body
        tree = ET.ElementTree(doc)
        tree.write(doc_dir_path + "%d.xml"%(i), encoding = doc_encoding, xml_declaration = True)
        i += 1
    
if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read('../config.ini', 'utf-8')
    # http://news.sohu.com/1/0903/61/subject212846158.shtml
    # http://news.sohu.com/1/0903/61/subject212846158_1091.shtml
    # http://news.sohu.com/1/0903/61/subject212846158_1090.shtml
    # http://news.sohu.com/1/0903/61/subject212846158_1089.shtml
    # ...
    # http://news.sohu.com/1/0903/61/subject212846158_993.shtml
    root = 'http://news.sohu.com/1/0903/61/subject212846158'

    # 获得获得所有的页面
    news_pool = get_news_pool(root, 1091, 1081)

    # 抓取每个新闻界面
    crawl_news(news_pool, 140, config['DEFAULT']['doc_dir_path'], config['DEFAULT']['doc_encoding'])
    print('done!')