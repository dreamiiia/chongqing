# -*- coding:utf-8 -*-
# 一个爬取重庆房价的爬虫（从安居客上）
# 加上多进程 学习multprocessing
import json
import time
import functools
import csv
from multiprocessing.pool import Pool, ThreadPool
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor  # 多线程
import threading

import requests
from lxml import etree


def crawler(page):
    # 获取url页面
    url = 'https://cq.fang.anjuke.com/loupan/all/p{}/'.format(page)
    html = requests.get(url).text

    # 解析页面 加上数据清洗吧
    houses_obj = etree.HTML(html)
    houses = houses_obj.xpath('//div[@class="list-contents"]//div[@class="item-mod"]')
    house_lists = []

    for house in houses:
        name = house.xpath('div[@class="infos"]//a[@class="items-name"]/text()')  # 它输出的就是一个列表咧，而且xpath语法中[1]为第一个
        address = house.xpath('div[@class="infos"]/p[@class="address"]/a/text()')
        type = house.xpath('div[@class="infos"]/p[2]/a/text()')  # 户型
        area = house.xpath('div[@class="infos"]/p[2]/span/text()') # 平米
        tag = house.xpath('div[@class="infos"]//div[@class="tag-panel"]/*/text()')
        # 价格要处理一波 有一些是售价待定的
        price = house.xpath('div[@class="favor-pos"]/p[1]//text()')

        try:
            # 还是要这样进行数据清理一下
            name = name[0] if name else ''
            address = address[0].replace('\xa0', '').replace('[', '').replace(']', ' ') if address else ''
            type = "/".join(type) if type else ''
            area = area[0][5:] if area else ''
            tag = ",".join(tag) if tag else ''
            price = " ".join(price) if price else ''
        except Exception as e:
            print(e)

        house_lists.append([name, address, type, area, tag, price])

        # house_list.append(
        #     {'姓名': name, '地址': address, '户型': type, '区域': area, '标签': tag, '价格': price}
        # )

    return house_lists


# CSV文件写入
def csv_writer(lists):
    with open('chongqing.csv', 'a', encoding="utf-8", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['姓名', '地址', '户型', '区域', '标签', '价格'])
        for list in lists:
            writer.writerow(list)


# txt文件
def txt_writer(content_dict):
    with open('chongqing.txt', 'a', encoding="utf-8") as c:
        c.write(json.dumps(content_dict, ensure_ascii=False) + '\n')  # dumps将字典转变为字符串


# 写一个记录时间的装饰器
def timer(func):
    @functools.wraps(func)
    def timered(*pargs, **kargs):
        print('程序{}运行开始'.format(func.__name__))
        time_start = time.clock()
        result = func(*pargs, **kargs)
        print('运行结束')
        print('运行结束{0} 总花费时间{1} s'.format(func.__name__,time.clock()-time_start))
        return result
    return timered


# 依序
@timer
def main_1(pages):
    for page in range(pages):
        print("正在进行任务{}".format(page))
        house_lists = crawler(page)
        print(house_lists)
        print('任务{}结束'.format(page))
    return house_lists

    # csv_writer(house_lists)
    # for i in house_list:
    #     txt_writer(i)
    #     print(i)


#  使用multiprocessing.Pool 进程池
@timer
def main_process(pages):
    # 多进程 map
    with Pool(4) as p:
        # p = Pool(3)
        result = p.map(crawler, range(pages))

    # for i in range(page):
    #     p.apply_async(main, args=(i,))
    # print('等待所有子进程完成')
    # p.close()
    # p.join()
    # print('所有子进程完成')


@timer
def main_process_thread(pages):
    # 多线程池
    with ThreadPool(4) as p:
        result = p.map(crawler, range(pages))


# concurrent.futures的ThreadPoolExecutor
@timer
def main_ThreadPoolExecutor(pages):
    with ThreadPoolExecutor(4) as executor:
        executor.map(crawler, range(pages))


if __name__ == '__main__':
    # main_1(10)
    main_process(20)
    main_process_thread(20)
    main_ThreadPoolExecutor(20)




