import rethinkdb as r
from datetime import datetime, date, time
from pytz import timezone

c = r.connect()
r.db("themis").table_drop("pages").run(c)
r.db("themis").table_drop("crawledUrls").run(c)
r.db("themis").table_create("pages").run(c)
r.db("themis").table_create("crawledUrls").run(c)

cursor = r.db("themis").table("pagesNew2").run(c)
titles = set()
for document in cursor:
    title = document['title']
    content = document['content']
    author = document['author']
    url = document['uri']
    date = document['date']
    if (title in titles):
        print("breaked")
        break
    titles.add(title)
    date = datetime.strptime(date, "%Y%m%d")
    utc = timezone('UTC')
    date = utc.localize(date)
    data = {
       'title' : title,
       'content' : content,
       'author' : author[3:0],
       'url' : url,
       'date' : date
    }
    crawled = {
       'url' : url,
       'crawled' : 1
    }
    r.db("themis").table("pages").insert(data).run(c)
    r.db("themis").table("crawledUrls").insert(crawled).run(c)
