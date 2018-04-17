import json
import urllib.request
import urllib.parse
from urllib.error import URLError
from socket import timeout
from pprint import pprint
from tqdm import tqdm
from ratelimit import rate_limited
import psycopg2
from psycopg2.extensions import TransactionRollbackError
from joblib import Parallel, delayed
import datetime


langs = ['en', 'ru']
sep = '||'
lang_seealso = {'en': 'See also', 'ru': 'См. также'}
lang_ref = {'en': 'References', 'ru': 'Примечания'}
lang_bib = {'en': 'Bibliography', 'ru': 'Литература'}
lang_source = {'en': 'Sources', 'ru': '???'}
lang_further = {'en': 'Further reading', 'ru': '???'}
lang_ext = {'en': 'External links', 'ru': 'Ссылки'}
disambiguations = ['(disambiguation)', '(значения)']


@rate_limited(16)
def call_api(lang, **params):
    params["format"] = "json"
    if 'page' in params:
        params['page'] = urllib.parse.quote(params['page'].encode("utf8"))
    qs = "&".join("%s=%s" % (k, v) for k, v in params.items())
    url = "https://" + lang + ".wikipedia.org/w/api.php?%s" % qs
    return json.loads(urllib.request.urlopen(url, timeout=20).read())


def get_sections(page):
    sections = page['sections']
    return {s['line']: int(s['index']) for s in sections if s['index'].isdigit()}


def get_categories(page):
    return [cat['*'] for cat in page['categories'] if '*' in cat]


def get_templates(page):
    return [tmpl['*'] for tmpl in page['templates'] if '*' in tmpl and tmpl['ns'] == 10]


def get_internal_links(lang, title, section, ns):
    if section:
        links = call_api(lang=lang, action='parse', prop='links', page=title, section=section, redirects=True)['parse']['links']
    else:
        links = call_api(lang=lang, action='parse', prop='links', page=title, redirects=True)['parse']['links']
    return [link['*'] for link in links if '*' in link and link['ns'] == ns]


def get_external_links(lang, title, section):
    if section:
        links = call_api(lang=lang, action='parse', prop='externallinks', page=title, section=section, redirects=True)['parse']['externallinks']
    else:
        links =  call_api(lang=lang, action='parse', prop='externallinks', page=title, redirects=True)['parse']['externallinks']
    return links


def get_redirect(lang, title):
    response = call_api(lang=lang, action='parse', prop='revid', page=title, redirects=True)
    if 'error' in response and response['error']['code'] in ('missingtitle', 'invalidtitle'):
        return None
    return response['parse']['title']


def get_lang_links(lang, title, cursor):
    cursor.execute('''SELECT lang_titles FROM title_map WHERE title = %s''', (lang+sep+title,))
    links = cursor.fetchone()
    if links:
        for link in links[0]:
            if link.split(sep)[0] != lang:
                save_link(lang, title, link, 'lang', cursor)
        return links[0]

    response = call_api(lang=lang, action='parse', prop='langlinks', page=title, redirects=True)
    if 'error' in response and response['error']['code'] in ('missingtitle', 'invalidtitle'):
        cursor.execute('''INSERT INTO title_map(title, lang_titles) 
                              VALUES(%s, %s) ON CONFLICT DO NOTHING''', (lang + sep + title, []))
        return []
    response = response['parse']
    links = []
    for link in response['langlinks']:
        if link['lang'] in langs:
            link_title = get_redirect(link['lang'], link['*'])
            if link_title:
                links.append(link['lang'] + sep + link_title)
    for link in links:
        save_link(lang, title, link, 'lang', cursor)
    links = [lang+sep+response['title']] + links
    cursor.execute('''INSERT INTO title_map(title, lang_titles) 
                      VALUES(%s, %s) ON CONFLICT DO NOTHING''', (lang+sep+title, links))
    return links


def save_link(lang, fr, to, type, cursor):
    for disambiguation in disambiguations:
        if disambiguation in to.lower():
            return
    fr = lang+sep+fr
    if type in ('seealso', 'intext'):
        links_to = get_lang_links(lang, to, cursor)
        for to in links_to:
            if fr != to:
                cursor.execute('''INSERT INTO links(from_title, to_title, type)
                                  VALUES(%s, %s, %s) ON CONFLICT DO NOTHING''', (fr, to, type))
    else:
        if type in ('cat', 'tpl'):
            to = lang+sep+to
        cursor.execute('''INSERT INTO links(from_title, to_title, type)
                          VALUES(%s, %s, %s) ON CONFLICT DO NOTHING''', (fr, to, type))


def save_text(title, text, cursor):
    cursor.execute('''INSERT INTO texts(title, txt)
                          VALUES(%s, %s) ON CONFLICT DO NOTHING''', (title, text))


def parse_article(title, conn_str):
    print(title)
    lang, title = title.split(sep)
    db = psycopg2.connect(conn_str)
    cursor = db.cursor()

    try:
        response = call_api(lang=lang, action='parse', prop='wikitext|sections|categories|templates', page=title, redirects=True)
        if 'error' in response and response['error']['code'] in ('missingtitle', 'invalidtitle'):
            cursor.execute('''DELETE FROM links WHERE to_title = %s''', (lang + sep + title,))
            db.commit()
            db.close()
            return
        page = response['parse']

        title = page['title']
        text = page['wikitext']['*']

        sections = get_sections(page)
        categories = get_categories(page)
        templates = get_templates(page)

        seealso_str = lang_seealso[lang]
        see_also_links = set(get_internal_links(lang, title, sections[seealso_str], 0) if seealso_str in sections else [])
        ext_links = set(get_external_links(lang, title, None))
        get_lang_links(lang, title, cursor)

        text_links = set(get_internal_links(lang, title, None, 0))
        text_links = list(text_links - see_also_links)

        for link in see_also_links:
            save_link(lang, title, link, 'seealso', cursor)
        for link in ext_links:
            save_link(lang, title, link, 'ext', cursor)
        for link in text_links:
            save_link(lang, title, link, 'intext', cursor)
        for link in categories:
            save_link(lang, title, link, 'cat', cursor)
        for link in templates:
            save_link(lang, title, link, 'tpl', cursor)

        save_text(lang + sep + title, text, cursor)
    except TransactionRollbackError as e:
        print('!!!TransactionRollbackError ' + lang + sep + title)
    except URLError as e:
        print('!!!URLError ' + lang + sep + title)
    except timeout as e:
        print('!!!timeout ' + lang + sep + title)
    except KeyError as e:
        print('!!!KeyError ' + str(e) + lang + sep + title)
    db.commit()
    db.close()


if __name__ == "__main__":
    conn_str = 'host=localhost dbname=postgres user=postgres password=postgres'
    db = psycopg2.connect(conn_str)
    cursor = db.cursor()

    cursor.execute('''
      CREATE TABLE IF NOT EXISTS links (
        id SERIAL,
        from_title TEXT,
        to_title TEXT,
        type TEXT,
        PRIMARY KEY (from_title, to_title)
      );
    ''')
    cursor.execute('''
          CREATE TABLE IF NOT EXISTS title_map (
            title TEXT,
            lang_titles TEXT[],
            PRIMARY KEY (title)
          );
        ''')
    cursor.execute('''
          CREATE TABLE IF NOT EXISTS texts (
            title TEXT,
            txt TEXT,
            PRIMARY KEY (title)
          );
        ''')
    cursor.execute('''CREATE INDEX IF NOT EXISTS from_index ON links (from_title)''')
    cursor.execute('''CREATE INDEX IF NOT EXISTS to_index ON links (to_title)''')
    db.commit()

    # https://en.wikipedia.org/wiki/Wikipedia:Getting_to_Philosophy
    parse_article('en||Machine learning', conn_str)
    parse_article('ru||Машинное обучение', conn_str)

    while True:
        cursor.execute('''SELECT DISTINCT to_title FROM
                    (SELECT queue.to_title, id FROM links
                        INNER JOIN (
                            SELECT DISTINCT to_title FROM links
                            WHERE type IN ('seealso', 'intext', 'lang')
                            EXCEPT (SELECT title FROM texts)
                        ) queue on links.to_title = queue.to_title 
                        ORDER BY id
                        LIMIT 128
                    ) filtered_queue''')
        titles = cursor.fetchall()
        Parallel(n_jobs=8, verbose=1)(delayed(parse_article)(title[0], conn_str) for title in titles)
        #res = [parse_article(title[0], conn_str) for title in titles]
