from urllib.request import build_opener
from urllib.request import HTTPCookieProcessor
from bs4 import BeautifulSoup
from http.cookiejar import CookieJar
import rethinkdb as r
import re
import random
from datetime import datetime, date, time
from pytz import timezone

articles = set() # article pages we want to crawl, but have not yet!
articlesCrawled = set() # articles we already crawled
conn = r.connect("localhost", 28015)

def getData(bsObj):
    content = []
    storyContentList = bsObj.findAll("p", {"class":"story-body-text story-content"})
    for paragraph in storyContentList:
        content.append(paragraph.get_text())
    if (content == []):
        print("no content")
        return

    titleFull = bsObj.find("meta", {"name":"hdl"})
    if (not titleFull is None):
        title = titleFull.attrs['content']
    else:
        print("no title")
        return

    uriFull = bsObj.find("link", {"rel":"canonical"})
    if (not uriFull is None):
        uri = uriFull.attrs['href']
        if (uri in articlesCrawled):
            print("uri already there")
            return
    else:
        print("url not found")
        return

    authorFull = bsObj.find("meta", {"name":"byl"})
    if (not authorFull is None):
        author = authorFull.attrs['content']
    else:
        return
        print("author not found")

    dateFull = bsObj.find("meta", {"name":"ptime"})
    if (not dateFull is None):
        date = dateFull.attrs['content']
        date = datetime.strptime(date, "%Y%m%d%H%M%S")
        utc = timezone('UTC')
        date = utc.localize(date)
    else:
        return
        print("date not found")

    imageurlFull = bsObj.find("figure")
    imageurl = ""
    if (not imageurlFull is None):
        try:
            imageurl = imageurlFull.attrs['itemid']
        except:
            print("no pricture")

    data = {
       'title' : title,
       'content' : content,
       'author' : author[3:],
       'url' : uri,
       'date' : date,
       'image_url' : imageurl
    }
    setCrawled(uri, bsObj)
    saveToDB(data)

def saveToDB(item):
    global conn
    r.db("themis").table("pages").insert(item).run(conn)

def saveUrlInDB(url, isCrawled):
    global conn
    rawData = {
       'url' : url,
       'crawled' : isCrawled
    }
    r.db("themis").table("crawledUrls").filter(r.row["url"] == url).delete().run(conn)
    r.db("themis").table("crawledUrls").insert(rawData).run(conn)

def getRandomArticleUrl():
    global articles
    urlArray = random.sample(set(articles), 1)
    return urlArray[0]

def loadUrlsfromDB():
    global conn
    cursor = r.db("themis").table("crawledUrls").run(conn)
    for url in cursor:
        if url['crawled'] is 0:
            articles.add(url['url'])
        else:
            articlesCrawled.add(url['url'])
    print("%d articles found to crawl, %d already Crawled" % (len(articles), len(articlesCrawled)))

def buildBeautifulSoup(pageUrl):
    cj = CookieJar()
    opener = build_opener(HTTPCookieProcessor(cj))
    html = opener.open(pageUrl)
    bsObj = BeautifulSoup(html)
    return bsObj

def scanHomeForUrls():
    bsObj = buildBeautifulSoup("http://www.nytimes.com/")
    extractArticles(bsObj)

def setCrawled(url, bsObj):
    articlesCrawled.add(url)
    saveUrlInDB(url, 1)
    try:
        #only crashes if url is not in articles set which should NEVER happen
        articles.remove(url)
    except:
        print("failed to remove url from articles set, should NOT have happend")
    try:
        #try to get the real url
        uriFull = bsObj.find("link", {"rel":"canonical"})
        uri = uriFull.attrs['href']
        articlesCrawled.add(uri)
        saveUrlInDB(uri, 1)
    except:
        print("no canonical link found or no bsObj given")

def extractArticles(bsObj):
    global articles
    global articlesCrawled
    for link in bsObj.findAll("a", href=re.compile("^http://www.nytimes.com/[0-9][0-9][0-9][0-9]/[0-9][0-9]/[0-9][0-9]/")):
        if 'href' in link.attrs:
            if (link.attrs['href'] not in articles) and (link.attrs['href'] not in articlesCrawled):
                #We have found a new page
                newPage = link.attrs['href']
                splitArray = newPage.split()
                newPage = splitArray[0]
                articles.add(newPage)
                saveUrlInDB(newPage, 0)

def main():
    global articles
    global articlesCrawled
    loadUrlsfromDB()
    if len(articles) is 0:
        scanHomeForUrls()
    while True:
        url = getRandomArticleUrl()
        try:
            bsObj = buildBeautifulSoup(url)
        except:
            print("building soup failed with url: %s" % url)
            articlesCrawled.add(url)
            saveUrlInDB(url, 1)
            try:
                #only crashes if url is not in articles set which should NEVER happen
                articles.remove(url)
            except:
                print("failed to remove url from articles set, should NOT have happend")
            continue
        extractArticles(bsObj)
        getData(bsObj)
        print("another one")

main()
