# coding=utf-8
import os
import sys
import time
import datetime
import urllib
import MySQLdb
from bs4 import BeautifulSoup
import requests
import random

from Mysql_Config_Fyh import data_to_mysql
from Mysql_Config_Fyh import logger


class BeiJingSearcher(object):
    def __init__(self):
        self.mysql_conn()
        self.headers = ''
        self.set_config()

    def set_config(self):
        self.headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0",
                        "Host": "www.bjsat.gov.cn",
                        'Accept-Encoding': 'gzip, deflate',
                        'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
                        'Connection': 'keep-alive',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                        }
        self.url = 'http://www.bjsat.gov.cn/bjsat/office/jsp/qsgg/query.jsp?'
        self.host = 'http://www.bjsat.gov.cn/bjsat/office/jsp/qsgg/'
        self.begin_time = self.get_date(7)
        self.end_time = time.strftime("%Y-%m-%d")
        self.last_update_time = time.strftime("%Y-%m-%d %H:%M:%S")
        self.province = u'北京市'

    def mysql_conn(self):
        self.conn = MySQLdb.connect(host='172.16.0.76', port=3306, user='fengyuanhua', passwd='!@#qweASD',
                                    db='taxplayer', charset='utf8')
        self.cursor = self.conn.cursor()

    def get_request(self, url, params=None):
        for k in range(15):
            try:
                r = requests.get(url, headers=self.headers, params=params)
                r.encoding = 'gbk'
                res = BeautifulSoup(r.text, 'html5lib')
                return res
            except:
                if k == 14:
                    print u'请求服务器重试结束', url
                    os._exit(1)
                else:
                    print u'请求服务器重试，第' + str(k) + u'次重试'

    def post_request(self, url, params):
        for k in range(15):
            try:
                r = requests.post(url, params=params, headers=self.headers)
                r.encoding = 'gbk'
                res = BeautifulSoup(r.text, 'html5lib')
                return res
            except:
                if k == 14:
                    print u'请求服务器重试结束', url
                    os._exit(1)
                else:
                    print u'请求服务器重试，第' + str(k) + u'次重试'

    def get_date(self, days):
        self
        return (datetime.datetime.now() - datetime.timedelta(days=days)).strftime('%Y-%m-%d')

    def log(self, message):
        self
        log_name = 'Taxplayer_BeiJing.log'
        logger(log_name, message)

    def run(self):
        log_name = 'Taxplayer_BeiJing.log'
        repeat_time = 0
        res_1 = searcher.get_request(self.url)
        content = res_1.find(bgcolor='#f6f9fc')
        fbdw_content = content.find('select')
        fbdw_list = fbdw_content.find_all('option')
        nsrlx_content = content.find_all('select')[1]
        nsrlx_list = nsrlx_content.find_all('option')
        for fbdw in fbdw_list[1:]:
            fbdw = fbdw.text.strip()
            fbdw_1 = fbdw.encode('gbk')
            # fbdw_2 = urllib.quote(fbdw.encode('gbk'))
            for nsrlx in nsrlx_list[1:]:
                nsrlx = nsrlx.text.strip()
                nsrlx_1 = nsrlx.encode('gbk')
                # nsrlx_2 = urllib.quote(nsrlx.encode('gbk'))
                params = {
                    'BeginTime': self.begin_time,
                    'EndTime': self.end_time,
                    'fbdw': fbdw_1,
                    'nsrlx': nsrlx_1,
                }
                res_2 = searcher.post_request(self.url, params)
                table = res_2.find(bgcolor='#d5e2f3')
                page_span = table.findAll('span', {'class': 'font_spci01'})
                if page_span:
                    page_num = int(page_span[0].text.split(u'共')[1].split(u'页')[0])
                    if page_num:
                        for i in range(1, page_num + 1):
                            params = {
                                'BeginTime': self.begin_time,
                                'EndTime': self.end_time,
                                'fbdw': fbdw_1,
                                'nsrlx': nsrlx_1,
                                'page_num': str(i),
                            }
                            res_3 = searcher.post_request(self.url, params=params)
                            # print 'res_3', res_3
                            table = res_3.find(bgcolor='#d5e2f3')
                            tr_list = table.findAll('tr')
                            if len(tr_list) > 3:
                                for tr in tr_list[2:-1]:
                                    tds = tr.findAll('td')
                                    # print 'tds', tds
                                    dwmc = tds[0].text.strip()
                                    nsrsbh = tds[1].text.strip()
                                    fzrxm = tds[2].text.strip()
                                    zjhm = tds[3].text.strip()
                                    fbrq = tds[5].text.strip()
                                    href = tds[0].encode('utf8').split('href="')[1].split('"')[0]
                                    url_1 = self.host + href
                                    res_4 = searcher.get_request(url_1)
                                    table = res_4.findAll('table', {'bgcolor': '#d5e2f3'})
                                    if table:
                                        trs = table[0].findAll('tr')
                                        jydd = trs[5].findAll('td')[1].text.strip()
                                        qssz = trs[6].findAll('td')[1].text.strip()
                                        qsje = trs[7].findAll('td')[1].text.strip()
                                        xqsje = trs[7].findAll('td')[1].text.strip()
                                        sql = "insert into taxplayer_bj_new VALUES('%s','%s','%s','%s','%s'," \
                                              "'%s','%s','%s','%s','%s','%s','%s','%s','%s')" % \
                                              (dwmc, nsrlx, nsrsbh, fzrxm, zjhm, jydd, qssz, qsje, xqsje, url_1,
                                               fbdw, fbrq, self.province, self.last_update_time)
                                        repeat_time = data_to_mysql(log_name, 0, sql, repeat_time)
                                    else:
                                        self.log('no table')
                            else:
                                self.log('no data tr')
                    else:
                        self.log('page_num为0')
                else:
                    self.log('page_span没有获取到')
        self.log('repeat_time:' + str(repeat_time))


if __name__ == '__main__':
    searcher = BeiJingSearcher()
    searcher.run()
