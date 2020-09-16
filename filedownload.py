# _*_ coding: utf-8 _*_
# @Author: smiles
# @Time  : 2020/9/16 9:42
# @File  : filedownload.py

import os
import queue
import logging
import threading

import requests
from fake_useragent import UserAgent

# 日志配置
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s : %(message)s')


class DownloadThread(threading.Thread):
    def __init__(self, q):
        super().__init__()
        self.q = q

    def run(self):
        while 1:
            url = self.q.get()
            print(f'{self.name} begin download {url}')
            self.download_file(url)
            self.q.task_done()
            print(f'{self.name} download completed')

    def download_file(self, url):
        ua = UserAgent()
        headers = {'User-Agent': ua.random}
        logging.info('scraping %s...', url)
        # 保存文件名
        fname = os.path.basename(url) + '.html'
        try:
            res = requests.get(url, headers=headers, stream=True)
            if res.status_code == 200:
                # 流式下载
                with open(fname, 'wb') as f:
                    for chunk in res.iter_content(chunk_size=1024):
                        if not chunk:
                            break
                        f.write(chunk)
            else:
                logging.error('get invalid status code %s while scraping %s',
                              res.status_code,
                              url,
                              exc_info=True)
        except requests.RequestException:
            logging.error('error occurred while scraping %s',
                          ur,
                          exc_info=True)


if __name__ == '__main__':
    urls = [
        'http://www.baidu.com',
        'http://www.bing.com',
    ]

    q = queue.Queue()

    for url in urls:
        q.put(url)

    # 启动两个个线程（可改为线程池）
    for i in range(2):
        t = DownloadThread(q)
        t.setDaemon(True)
        t.start()

    q.join()
