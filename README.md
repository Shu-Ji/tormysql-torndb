# tormysql-torndb

async [torndb](https://github.com/bdarnell/torndb) with [TorMySQL](https://github.com/snower/TorMySQL) for tornado

Every method is same as torndb, and just add `@gen.coroutine` to your `get`、`post` method，and use `yield` before methods from torndb(such as `query`、`insert` etc.)：

```python
class MainHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        sql = """select host, user from user where user = %s"""
        data = yield db.query(sql, 'root')
        self.write({'data': data})
```

not tested!!!
## !!!use as your own risk!!!
## !!!use as your own risk!!!
## !!!use as your own risk!!!

# full example: myapptest.py

```python

import tornado.ioloop
import tornado.web
from tornado import gen

from tormysql_torndb import Connection


db = Connection('127.0.0.1', 3306, 'mysql', 'root', 'mysql123')


class MainHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        sql = """select host, user from user where user = %s"""
        data = yield db.query(sql, 'root')
        self.write({'data': data})


class HelloHandler(tornado.web.RequestHandler):
    @gen.coroutine
    def get(self):
        sql = """select sleep(10)"""
        data = yield db.query(sql)
        self.write({'data': data})


def make_app():
    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/hello", HelloHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
```
