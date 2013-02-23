# coding: utf-8
import urllib2, urllib
import socket
import lxml.html           
import simplejson as json
import datetime
import csv
from BeautifulSoup import BeautifulSoup
from pymongo import Connection

socket.setdefaulttimeout(10)


def writeline(d, keys):
    arr = []
    for k in keys:
        arr.append(unicode(d[k]))
    return ('\t'.join(arr)).encode('utf8', 'replace')

keys=['postdate', 'url', 'name', 'photo_url']


class MedblogParser:
    def __init__(self):
        self.conn = Connection()
        self.db = self.conn['medblog']
        self.ucoll = self.db['users']
        self.rcoll = self.db['regions']
        self.scoll = self.db['stats']
        self.pcoll = self.db['posts']
        self.tcoll = self.db['themes']
        self.ccoll = self.db['comments']
        pass

    def parse_post_list(self):    
        print '\t'.join(keys)
        f = urllib2.urlopen("http://blog.kremlin.ru/")
        html = f.read()
        root = lxml.html.fromstring(html)
        last_ps = ""
        for td in root.cssselect("div[class='pagination'] a[class='page']"):
            last_ps = td.attrib['href']    
        last_page = int(last_ps.rsplit('=', 1)[1]) + 1
    
        for i in range(1, last_page, 1):
            durl = 'http://blog.kremlin.ru/?page=%d'
            data = self.parse_listpage(durl % i, i)

    def parse_postpage(self, url):
        f = urllib2.urlopen(url)
        h = f.read()
        soup = BeautifulSoup(h)
        last_ps = ""
        tda = soup.find("div", {'class' : 'pagination'})
        if tda is not None:
            tda = tda.findAll("a", {'class' : 'page'})
        else: 
            item = self.pcoll.find_one({'url' : url})
            item['num_com'] = 0
            item['comments_parsed'] = True
            self.pcoll.save(item)
            return
        for td in tda:
            last_ps = td['href']    
        last_page = int(last_ps.rsplit('=', 1)[1]) + 1
        ncom = soup.find('h2', {'id': 'comments'}).string
        ncom_int = int(ncom.split()[1].strip(')').strip('('))
        item = self.pcoll.find_one({'url' : url})
        item['num_com'] = ncom_int
        self.pcoll.save(item)
        if item['comments_parsed'] == True: return
        for i in range(1, last_page, 1):
            self.parse_comments(url + '/asc?page=%d' %(i), url)
        item['comments_parsed'] = True
        self.pcoll.save(item)
        

    def parse_comments(self, url, posturl):
        print url
        f = urllib2.urlopen(url)
        h = f.read()
        soup = BeautifulSoup(h)
        last_ps = ""
        comments = soup.findAll("li", {'class' : 'comment'})
        for comment in comments:
            if not comment.has_key('id'): continue
            d = {'id' : comment['id']}
            comm = self.ccoll.find_one({'id' : comment['id']})
            if comm is not None: continue
            theme = comment.find('a', {'class' : 'comment_theme'})
            if theme is not None: 
                d['theme_id'] = int(theme['href'].rsplit('/', 1)[1])
                d['theme_name'] = theme.string.strip()
            author = comment.find('div', {'class' : 'author'})
            d['posturl'] = posturl
            d['comment_type'] = 'post'
            d['author_id'] = int(author.find('a')['href'].rsplit('/', 1)[1])
            d['author_name'] = author.find('a').string
            d['author_region'] = author.findAll(text=True)[-1].string.strip(',').strip()
            d['date'] = comment.find('div', {'class' : 'date'}).string.strip()
            d['text'] = str(comment.find('div', {'class' : 'text'}).contents)
            self.ccoll.save(d)

    def parse_listpage(self, url, npage=1):
        f = urllib2.urlopen(url)
        h = f.read()
        soup = BeautifulSoup(h)
        n = 0
        if npage == 1:
            for tr in soup.findAll('div', {'class' : 'right-column'}):            
                namet = tr.find('h2')        
                if namet is None: continue
                namet = namet.find('a')
                mlinks = 'http://blog.kremlin.ru/static/styles/images/face.jpg'
                links = tr.find('h2').find("a")
                divdate = tr.find("div", {'class' : 'date'})    
                if len(namet) == 0: continue
                posturl = 'http://blog.kremlin.ru' + links['href']
                d = self.pcoll.find_one({'url' : posturl})
                if not d: d = {}
                else: continue
                d = {'name' : namet.string.strip(), 'url' : posturl, 
                    'photo_url' : mlinks, 
                    'postdate' : divdate.string.strip(), 'comments_parsed' : False}   
                self.pcoll.save(d)
                self.parse_postpage(posturl)
                print 'processed', 'http://blog.kremlin.ru' + links['href']

        for tr in soup.findAll('li', {'class' : 'post short'}):    
            namet = tr.find('h2')        
            if namet is None: continue
            namet = namet.find('a')
            mlinks = tr.find("a").find('img')
            links = tr.find("a")
            divdate = tr.find("div", {'class' : 'date'})        
            if len(namet) == 0: continue
            posturl = 'http://blog.kremlin.ru' + links['href']
            item = self.pcoll.find_one({'url' : posturl})
            if item is None: item = {'comments_parsed' : False}
            d = {'name' : namet.string.strip(), 'url' : posturl, 
                'photo_url' : mlinks['src'], 
                'postdate' : divdate.string.strip()}   
            item.update(d)
            self.pcoll.save(item)
            print 'processed', posturl
            if item['comments_parsed'] == True: continue
            self.parse_postpage(posturl)

    def parse_theme_comments(self, url, themeurl):
        print url
        f = urllib2.urlopen(url)
        h = f.read()
        soup = BeautifulSoup(h)
        last_ps = ""
        comments = soup.findAll("li", {'class' : 'comment'})
        for comment in comments:
            if not comment.has_key('id'): continue
            d = {'id' : comment['id']}
            comm = self.ccoll.find_one({'id' : comment['id']})            
            if comm is not None: continue
            cpost = comment.find('a', {'class' : 'comment_post'})
            if cpost is not None: continue
            d['themeurl'] = themeurl
            d['comment_type'] = 'theme'
            author = comment.find('div', {'class' : 'author'})
            d['author_id'] = int(author.find('a')['href'].rsplit('/', 1)[1])
            d['author_name'] = author.find('a').string
            d['author_region'] = author.findAll(text=True)[-1].string.strip(',').strip()
            d['date'] = comment.find('div', {'class' : 'date'}).string.strip()      
            d['text'] = str(comment.find('div', {'class' : 'text'}).contents)#.encode('utf8')
            self.ccoll.save(d)


    def parse_theme(self, url):
        f = urllib2.urlopen(url)
        h = f.read()
        soup = BeautifulSoup(h)
        last_ps = ""
        tda = soup.find("div", {'class' : 'pagination'})
        if tda is not None:
            tda = tda.findAll("a", {'class' : 'page'})
        else: 
            return
        for td in tda:
            last_ps = td['href']    
        last_page = int(last_ps.rsplit('=', 1)[1]) + 1
        item = self.tcoll.find_one({'url' : url})
        if item['comments_parsed'] == True: return
        startp = 1
        if item['id'] == 6: startp = 29
        for i in range(startp, last_page, 1):
            self.parse_theme_comments(url + '/asc?page=%d' %(i), url)
        item['comments_parsed'] = True
        self.tcoll.save(item)
        

    def parse_themes(self, deep=False):
        f = urllib2.urlopen("http://blog.kremlin.ru/themes/")
        h = f.read()
        soup = BeautifulSoup(h)
        n = 0
        summ = 0
        for tr in soup.find('div', {'id': 'body'}).findAll('h3'):
            link = tr.find('a')        
            num_c = tr.find('span', {'class' : 'comment-count'}).string.strip('(').strip(')')
            turl = 'http://blog.kremlin.ru' + link['href']
            item = self.tcoll.find_one({'url' : turl})
            if item is None: item = {'comments_parsed' : False}
            d = {'name' : link.string.strip(), 'url' : turl, 'num_com' : int(num_c), 'id' : int(link['href'].rsplit('/', 1)[1])}
            item.update(d)
            self.tcoll.save(item)
            print turl
            summ +=  int(num_c)
            if deep:
                self.parse_theme(turl)


    def update_post_dates(self):
        all = []
        d = {u'января' : 1, u'февраля' : 2, u'марта' : 3, u'апреля' : 4, u'мая' : 5, u'июня' : 6, u'июля' : 7, u'августа' : 8, u'сентября' : 9, u'октября' : 10, u'ноября': 11, u'декабря': 12}
        for o in self.pcoll.find():
            all.append(o)
        for o in all:
            postdate = o['postdate']
            day, m, year = postdate.split()
            thed = datetime.datetime(year=int(year), month=d[m], day=int(day))
            o['pdate'] = thed
            self.pcoll.save(o)

    def fix_comments(self):
        all = []
        d = {u'января' : 1, u'февраля' : 2, u'марта' : 3, u'апреля' : 4, u'мая' : 5, u'июня' : 6, u'июля' : 7, u'августа' : 8, u'сентября' : 9, u'октября' : 10, u'ноября': 11, u'декабря': 12}
        all = self.ccoll.find()
        i = 0
        for o in all:
            i += 1
            if i % 1000 == 0:
                print i
            postdate = o['date']
            day, m, year, hm = postdate.split()
            hour, minut = hm.split(':')
            thed = datetime.datetime(year=int(year), month=d[m], day=int(day), hour=int(hour), minute=int(minut))
            if o.has_key('themeurl'): 
                o['theme_id'] = int(o['themeurl'].rsplit('/', 1)[1])
            else: 
                if o.has_key('theme_id'): o['themeurl'] = 'http://blog.kremlin.ru/theme/%d' % (o['theme_id'])
            reg = o['author_region'] if o['author_region'] != o['author_name'] else u'Неизвестно'
            o['pdate'] = thed
            o['author_region'] = reg
            self.ccoll.save(o)

    def add_stat_record(self, y, m, ind_key, value):
        dkeys = {'comments' : u'Все комментарии', u'theme_com' : u'Комментарии по темам', u'post_com' : u'Комментарии к постам'}
        o = self.scoll.find_one({'y' : y, 'm' : m, 'ind_key' : ind_key})
        if o: 
            o['value'] = value
        else:
            o = {'y' : y, 'm' : m, 'ind_key' : ind_key, 'value': value, 'ind_name' : dkeys[ind_key]}
        self.scoll.save(o)
        
    def generate_stats_table(self):
        years = {}
        all = self.ccoll.find()
        total_u = 0
        total_c = 0
        for o in all:
            y = o['pdate'].year
            m = o['pdate'].month
            v = years.get(y, None)
            if v is not None:
                v2 = v.get(m)
                if v2 is None:
                    v[m] = {'comments': 1, 'post_com' : 0, 'theme_com' : 0}
                else:
                    v[m]['comments'] += 1
                key = 'post_com' if o['comment_type'] == "post" else 'theme_com'
                v[m][key] +=  1
            else:
                v = {}
                v[m] = {'comments' : 1, 'post_com' : 0, 'theme_com' : 0}   
                key = 'post_com' if o['comment_type'] == "post" else 'theme_com'
                v[m][key] +=  1
            years[y] = v        
        for y in years.keys():
            for m in years[y].keys():               
                for key in ['comments', 'theme_com', 'post_com']:
                    self.add_stat_record(y, m, key, years[y][m][key])
                    print '%d_%d\t%s\t%d' %(y, m, key, years[y][m][key])

        

    def calc_stats(self):
        years = {}
        all = self.pcoll.find()        
        for o in all:
            v = years.get(o['pdate'].year, None)
            if v is not None:
                v2 = v.get(o['pdate'].month)
                if v2 is None:
                    v[o['pdate'].month] = {'posts' : 1, 'comments': o['num_com']}
                else:
                    v[o['pdate'].month]['posts'] += 1
                    v[o['pdate'].month]['comments'] += o['num_com']                
            else:
                v = {}
                v[o['pdate'].month] = {'posts' : 1, 'comments': o['num_com']}   
            years[o['pdate'].year] = v
        for y in years.keys():
            for m in years[y].keys():
                if years[y][m]['posts'] > 0:
                    avg = float(years[y][m]['comments']) / years[y][m]['posts']
                else:
                    avg = 0.0
                print '%d_%d\t%d' %(y, m, years[y][m]['posts'])

    def theme_export(self, theme_id=58):
        all = self.ccoll.find({'theme_id' : theme_id})        
        for o in all:
            print o['clean_text'].encode('utf8')

    def comment_text_fix(self):
        all = self.ccoll.find()        
        ai = 0
        for o in all:
            block  = []
            ai += 1
            if ai % 1000 == 0: print ai
            parts = o['text'].split('<p>')
            for i in range(0, len(parts), 1):
                if i % 2 == 1: 
                    block.append(parts[i].split('</p>')[0])
                    text = '\n'.join(block) 
                    o['clean_text'] = text
                    self.ccoll.save(o)

    def generate_users_table(self):
        users = {}
        for o in self.ccoll.find():
            u = users.get(o['author_id'], None)
            if u is None:
                reg = o['author_region'] if o['author_region'] != o['author_name'] else u'Неизвестно'
                u = {'id' : o['author_id'], 'num_com' : 1, 'region' : reg, 'name' : o['author_name']}
            else:
                u['num_com'] += 1
            users[o['author_id']] = u
        for k in users.keys():
            u = self.ucoll.find_one({'id' : k})
            if u is not None:
                u.update(users[k])
                d = u
            else:
                d = users[k]
            print k
            self.ucoll.save(d)

    def generate_regions_table(self):
        regions = {}
        for o in self.ucoll.find():
            u = regions.get(o['region'], None)
            if u is None:
                u = {'name' : o['region'], 'users' : 1, 'num_com' : o['num_com']}
            else:
                u['users'] += 1
                u['num_com'] += o['num_com']
            regions[o['region']] = u
        for k in regions.keys():
            u = self.rcoll.find_one({'name' : k})
            if u is not None:
                u.update(regions[k])
                d = u
            else:
                d = regions[k]
            print d
            self.rcoll.save(d)

    def get_gender(self, name):
        params = urllib.urlencode({'text' : name.encode('utf8')})
        url = "http://apibeta.skyur.ru/names/parse/?%s" % params
        print url.encode('utf8')
        f = urllib2.urlopen(url.encode('utf8'))
        data = f.read()
        f.close()
        return json.loads(data)

    def update_gender(self):
        i = 0
        for o in self.ucoll.find({'gdata' : {'$exists' : False}}):
            i += 1
            if i % 10 == 0: print i
            data = self.get_gender(o['name'])
            o['gdata'] = data
            self.ucoll.save(o)
    
    def calc_gender_stats(self):
        i = 0
        parsed = 0
        total_c = 0
        not_parsed = 0
        gender = {}
        nform = {}
        comms = {}
        for o in self.ucoll.find({'gdata' : {'$exists' : True}}):
            i += 1
#            if i % 1000 == 0: print i
            if not o['gdata']['parsed']:
                not_parsed += 1
            else:
                parsed += 1
                v = gender.get(o['gdata']['gender'], 0)
                gender[o['gdata']['gender']] = v + 1
                v = nform.get(o['gdata']['format'], 0)
                nform[o['gdata']['format']] = v + 1
                v = comms.get(o['gdata']['gender'], 0)
                comms[o['gdata']['gender']] = v + o['num_com']
                total_c += o['num_com']
        print parsed, not_parsed
        for k in gender.keys():
            print k, comms[k] * 1.0 / gender[k], gender[k] * 100.0 / parsed, comms[k] * 100.0 / total_c
        for k in nform.keys():
            print k, nform[k] * 100.0 / parsed
        names = {}
        for o in self.ucoll.find({'gdata.parsed' : False}):
            name = o['name']
            v = names.get(name, 0)
            names[name] = v + 1
        thedict = sorted(names.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
        for key, value in thedict:
            print key.encode('utf8'), value

def find_missing():
    f = open('postlist.csv')
    nums = []
    n = 0
    for l in f:
        n += 1
        if n == 1: continue
        l = l.strip()
        parts = l.split('\t')
        url = parts[1]
        num = url.split('post/', 1)[1]
        num = int(num)
        nums.append(num)
    nums.sort()
    last = 0
    for n in nums:
        if last != 0: 
            if n - last > 1:
                for i in range(1, n - last, 1):
                    print 'http://blog.kremlin.ru/post/%d' %(n - i)
        last = n
            


if __name__ == "__main__":
    p = MedblogParser()
#    p.update_gender()
#    p.calc_gender_stats()
#    p.comment_text_fix()
#    p.generate_regions_table()
#    p.generate_stats_table()
    p.fix_comments()    
#    p.generate_users_table()
#    p.calc_user_stats()
#    p.calc_stats()
#    p.theme_export()
#    p.update_post_dates()
#    p.parse_post_list()
#    p.parse_themes(deep=True)
#    find_missing()

