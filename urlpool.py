import time

import pickle
import leveldb
import urllib.parse as urlparse


class UrlDB:
    """
    Use LevelDB to store URLs what have been done(success or faile)
    """
    status_failure = b'0'
    status_success = b'1'

    def __init__(self, db_name):
        self.name = db_name + '.urldb'
        self.db = leveldb.LevelDB(self.name)

    def set_success(self, url):
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            self.db.Put(url, self.status_success)
            s = True
        except:
            s = False
        return s

    def set_failure(self, url):
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            self.db.Put(url, self.status_failure)
            s = True
        except:
            s = False
        return s

    def has(self, url):
        if isinstance(url, str):
            url = url.encode('utf8')
        try:
            attr = self.db.Get(url)
            return attr
        except:
            pass
        return False


class UrlPool:
    """URL Pool for crawler to manage URLs
    """
    def __init__(self, pool_name):
        self.name = pool_name
        self.db = UrlDB(pool_name)

        self.waiting = {}  # {host: set([urls]),} 按 host 分组，记录等待下载的 url
        self.pending = {}  # {url: pended_time,}
        self.failure = {}  # {url: times,} 记录失败的 url 的次数
        self.failure_threshold = 3
        self.pending_threshold = 10
        self.waiting_count = 0  # self.waiting 字典里面 url 的个数
        self.max_hosts = ['', 0]
        self.hub_pool = {}  # {url: last_query_time,}
        self.hub_refresh_span = 0
        self.load_cache()

    def __del__(self):
        self.dump_cache()

    def load_cache(self):
        path = self.name + '.pkl'
        try:
            with open(path, 'rb') as f:
                self.waiting = pickle.load(f)
            cc = [len(v) for k, v in self.waiting.items()]
            print('saved pool loaded! urls:', sum(cc))
        except:
            pass

    def dump_cache(self):
        path = self.name + '.pkl'
        try:
            with open(path, 'wb') as f:
                pickle.dump(self.waiting, f)
            print('self.waiting saved!')
        except:
            pass

    def set_hubs(self, urls, hub_refresh_span):
        self.hub_refresh_span = hub_refresh_span
        self.hub_pool = {}
        for url in urls:
            self.hub_pool[url] = 0

    def set_status(self, url, status_code):
        if url in self.pending:
            self.pending.pop(url)

        if status_code == 200:
            self.db.set_success(url)
            return
        if status_code == 404:
            self.db.set_failure(url)
            return
        if url in self.failure:
            self.failure[url] += 1
            if self.failure[url] >= self.failure_threshold:
                self.db.set_failure(url)
                self.failure.pop(url)
            else:
                self.add(url)
        else:
            self.failure[url] = 1
            self.add(url)

    def push_to_pool(self, url):
        host = urlparse.urlparse(url).netloc
        if not host or '.' not in host:
            print('try to push to pool with bad url:', url, ', len of url:',
                  len(url))
            return False
        if host in self.waiting:
            if url in self.waiting[host]:
                return True
            self.waiting[host].add(url)
            # 记录 pool 中 url 最多的 host 以及 url 的数量
            if len(self.waiting[host]) > self.max_hosts[1]:
                self.max_hosts[1] = len(self.waiting[host])
                self.max_hosts[0] = host
        else:
            self.waiting[host] = set([url])
        self.waiting_count += 1
        return True

    def add(self, url, always=False):
        if always:
            return self.push_to_pool(url)
        pended_time = self.pending.get(url, 0)
        if time.time() - pended_time < self.pending_threshold:
            print('being download: ', url)
            return
        if self.db.has(url):
            return
        if pended_time:
            self.pending.pop(url)
        return self.push_to_pool(url)

    def addmany(self, urls, always=False):
        if isinstance(urls, str):
            print('urls is str !!!', urls)
            self.add(urls, always)
        else:
            for url in urls:
                self.add(url, always)

    def pop(self, count, hub_percent=50):
        print('\n\tmax of host:', self.max_hosts)

        # 取出的 url 有两种类型：hub=1，普通=0
        url_attr_url = 0
        url_attr_hub = 1
        # 1. 首先取出 hub，保证获取 hub 里面最新的 url
        hubs = {}
        hub_count = count * hub_percent // 100
        for hub in self.hub_pool:
            span = time.time() - self.hub_pool[hub]
            if span < self.hub_refresh_span:
                continue
            hubs[hub] = url_attr_hub
            self.hub_pool[hub] = time.time()
            if len(hubs) >= hub_count:
                break

        # 2. 取出普通的 url
        left_count = count - len(hubs)
        urls = {}
        for host in self.waiting:
            if not self.waiting[host]:
                continue
            url = self.waiting[host].pop()
            urls[url] = url_attr_url
            self.pending[url] = time.time()
            if self.max_hosts[0] == host:
                self.max_hosts[1] -= 1
            if len(urls) > left_count:
                break
        self.waiting_count -= len(urls)
        print('To pop: %s, hubs: %s, urls: %s, hosts: %s' %
              (count, len(hubs), len(urls), len(self.waiting)))
        urls.update(hubs)
        return urls

    def size(self):
        return self.waiting_count

    def empty(self):
        return self.waiting_count == 0
