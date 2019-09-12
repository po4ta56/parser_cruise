import asyncio
import aiohttp
import re
import requests
from datetime import  datetime
from bs4 import BeautifulSoup


LIMIT_CRUISE = 4


def get_user_agent():
    return 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36'


def get_request_headers():
    return {
        'User-Agent': get_user_agent(),
    }


def get_html(address):

    r = requests.get(address, headers=get_request_headers())
    if r.status_code == 200:
        return r.text
    else:
        return None


def parse(html):
    def init_cruise_data():
        return {
            'name': '',
            'itinerary': [],
        }

    def init_itinerary_item():
        return {
            'name': '',
            'days': 0,
            'dates': [],
        }

    cruises = []

    html_tree = BeautifulSoup(html, features='lxml')
    travel_boxs = html_tree.find_all('div', 'travel-box-container', limit=LIMIT_CRUISE)

    for travel_box in travel_boxs:
        cruise = init_cruise_data()
        header = travel_box.find('h3', 'travel-box-heading')
        cruise['name'] = header.span.a.string.strip()

        year_containers = travel_box.find_all('div', re.compile(r'showYear\d{4} yearContainer'), style=None)
        for year_container in year_containers:
            cruise_detail = year_container.find_all('div', 'row item_new')
            for row in cruise_detail:
                itinerary = init_itinerary_item()
                itinerary['name'] = row.find('p', 'cruise-route').contents[1].strip()
                itinerary['days'] = row.find('p', 'cruise-duration').contents[1].strip()
                itinerary['href'] = row.find('p', 'cruise-button').a['href']
                cruise['itinerary'].append(itinerary)
        cruises.append(cruise)
    return cruises


def parse_detail(html, itinerary):
    def init_itinerary_date_item():
        return {
            'ship': '',
            'price': 0,
        }
    html_tree = BeautifulSoup(html, features='lxml')
    date_items = html_tree.find_all('div', 'accordeon-panel-default')
    for item in date_items:
        date_item = init_itinerary_date_item()
        date = str(item.find('span', 'price-duration').string).strip()
        price_ship = item.find('div', 'price-ship')
        date_item['ship'] = price_ship.find('span', 'table-ship-name').string.strip()
        date_item['price'] = price_ship.find('span', 'big-table-font').string.strip()
        itinerary['dates'].append({
            date: date_item
        })


def sync_processing():
    root_url = 'https://www.lueftner-cruises.com'
    html = get_html(root_url+'/en/river-cruises/cruise.html')
    cruises = parse(html)
    for cruise in cruises:
        for itinerary in cruise['itinerary']:
            html = get_html(root_url+itinerary['href'])
            parse_detail(html, itinerary)

    return cruises


async def async_fetch(url, session):
    async with session.get(url, timeout=30) as response:
        if response.status == 200:
            return url, await response.text()
        else:
            return url, None


async def async_parse():
    root_url = 'https://www.lueftner-cruises.com'
    html = get_html(root_url+'/en/river-cruises/cruise.html')
    cruises = parse(html)
    itinerary_urls = dict()
    for cruise in cruises:
        for itinerary in cruise['itinerary']:
            itinerary_urls[root_url+itinerary['href']] = itinerary

    urls = list(itinerary_urls.keys())
    async with aiohttp.ClientSession(headers=get_request_headers()) as session:
        futures = []
        while urls:
            futures.append(async_fetch(urls.pop(), session))
        for future in asyncio.as_completed(futures):
            try:
                url, content = await future
            except:
                pass
            else:
                if content:
                    parse_detail(content, itinerary_urls[url])
    return cruises


def async_processing():
    return asyncio.run(async_parse())


def get_digits(text):
    return re.findall(r'[\d.,]+', text, re.MULTILINE)


def to_output_format(raw_data):
    '''
    Перобразование из формата:
    [{'name': 'Classical Rhine Cruise',
    'itinerary': [
        {
            'name': 'Amsterdam \t→ Basel',
            'days': '8 Days',
            'dates': [
                    {'24. Oct 2019 - 31. Oct 2019': {
                                            'ship': 'MS Amadeus Silver II',
                                            'price': '€ 1.769,00'}}],
            'href': '/en/river-cruises/cruise/show/classical-rhine-cruise-2019.html'},

        {
            'name': 'Basel \t→ Amsterdam',
            'days': '8 Days',
            'dates': [
                    {'31. Oct 2019 - 07. Nov 2019': {
                                            'ship': 'MS Amadeus Silver II',
                                            'price': '€ 1.569,00'}}],
            'href': '/en/river-cruises/cruise/show/classical-rhine-cruise-2019-r.html'}]
    }, ...]

    в формат:
    [{
    «name»: «Tulip Serenade»,
    «days»: 8,
    «itinerary»: [amsterdam,amsterdam,arnhem …..],
    «dates»:[
    {«2019-04-04»:{«ship»: «ms amadeus queen», «price»:1044.65},
    ...(остальные даты)}
    ],
    },..]
    '''

    result = []
    for cruise in raw_data:
        name = cruise['name']
        for itinerary in cruise['itinerary']:
            result_row = dict()
            result_row['name'] = name
            result_row['days'] = get_digits(itinerary['days'])[0]
            result_row['itinerary'] = [s.strip() for s in itinerary['name'].split('\t→')]
            result_row['dates'] = []
            for detail in itinerary['dates']:
                for key, data in detail.items():
                    cruise_date = key.split('-')[0].strip()
                    dates_item = dict()
                    dates_item['ship'] = data['ship']
                    dates_item['price'] = get_digits(data['price'])
                    result_row['dates'].append({cruise_date: dates_item})
            result.append(result_row)
    return result





if __name__=='__main__':
    '''
    start = datetime.now()
    raw_data = sync_processing()
    print(datetime.now()-start)
    '''

    start = datetime.now()
    raw_data = async_processing()
    print(to_output_format(raw_data))

    print(datetime.now()-start)
