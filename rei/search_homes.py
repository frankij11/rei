# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 21:02:39 2020

@author: kevin
"""

import requests_html
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
from tqdm import tqdm

try:
    from . import sdat
except:
    import sdat

redfin_dunkirk = '''https://www.redfin.com/stingray/api/gis-csv?al=3&max_price=650000&min_stories=1&num_homes=350&ord=redfin-recommended-asc&page_number=1&poly=-76.7469%2038.63292%2C-76.46564%2038.63292%2C-76.46564%2038.78948%2C-76.7469%2038.78948%2C-76.7469%2038.63292&school_rating=7-&school_types=1,2,3&sf=1,2,3,5,6,7&status=9&uipt=1,2,3,4,5,6&v=8'''
def redfin(url=None):
    if url == None: url = 'https://www.redfin.com/stingray/api/gis-csv?al=1&market=dc&max_price=500000&min_stories=1&num_homes=350&ord=redfin-recommended-asc&page_number=1&region_id=20065&region_type=6&sf=1,2,3,5,6,7&status=9&uipt=1,2,3,4,5,6&v=8'
    #url = 'https://www.redfin.com/city/20065/MD/Upper-Marlboro/filter/max-price=500k'
    with requests_html.HTMLSession() as session:
        r = session.get(url)
        #r.html.render()
        #data = pd.read_html(r.html.html)
        df = pd.read_csv(url)
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

        
    return df

def redfin2():
    url = 'https://www.redfin.com/zipcode/20772/filter/property-type=house,max-price=500k,min-sqft=2.5k-sqft,has-garage,basement-type=finished+unfinished,min-stories=2,viewport=39.00739:38.7725:-76.64966:-76.95007,no-outline'
    url = '/stingray/api/gis-csv?al=1&basement_types=0,1,3&gar=true&max_price=500000&min_listing_approx_size=2500&min_stories=2&num_homes=350&ord=redfin-recommended-asc&page_number=1&poly=-76.95007%2038.7725%2C-76.64966%2038.7725%2C-76.64966%2039.00739%2C-76.95007%2039.00739%2C-76.95007%2038.7725&sf=1,2,3,5,6,7&status=9&uipt=1&v=8'
    with requests_html.HTMLSession() as session:
        r = session.get(url)
        print(r.html.links)
        #r.html.render()
        #data = pd.read_csv(url)
        #data = pd.read_html(r.html.html)

def auction_hw():
    #url ='https://www.hwestauctions.com/assets/php4/tabbedWebsite.php'
    url = 'https://www.hwestauctions.com/schedule.v4.php'
    session = requests_html.HTMLSession()
    r = session.get(url)
    #r.html.render()
    data = pd.read_html(r.html.html)
    r.close()
    df = data[2]
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    df['source'] = url
    #pd.DataFrame()
    
    #for i in range(5):
    #    df[data[2][i][0]] = data[0][i][1:]
    #df[['Address','Post_Date']] = df['Address, City (First Pub)'].str.split('(',expand=True)
    #df['Post_Date'] = df['Post_Date'].str.replace(")", "")

    return df

def auction_ac():
    url ='https://realestate.alexcooper.com/foreclosures?limit=100&auction_county=Prince+George%27s+County'
    session = requests_html.HTMLSession()
    r = session.get(url)
    r.html.render()
    soup = BeautifulSoup(r.html.html, "html.parser")
    dates_ = soup.find_all('div', attrs={"class": "full-date"})
    dates = []
    for row in dates_:
        row = row.text.replace("\t", "").replace("|", "").replace("\n", " ")
        dates.append(row) 
    data = soup.find_all('div', attrs={"class": "foreclosure-lot"})
    r.close()
    res=[]
    d = 0
    prev_date =pd.to_datetime(0)
    for row in data:
        cancelled ="cancelled" in row['class']
        row = row.text.strip().splitlines()[0]
        cur_date = pd.to_datetime(dates[d] + row[0:8])
        if cur_date < prev_date: d += 1
        res.append([
                   pd.to_datetime(dates[d] + row[0:8]),
                   row[9: row.find('Dep.')-1],
                   row[row.find('Dep.')+5:],
                   cancelled
                   ])
        prev_date = pd.to_datetime(dates[d] + row[0:8])
    df = pd.DataFrame(res, columns=['sale_time', 'Address', 'deposit_in_$k,000', 'cancelled'])
    adrs = df["Address"].str.split(",", expand = True) 
    df.Address = adrs[:][0].str.replace(".","")
    df.City = adrs[:][1]
    df.zip = adrs[:][2]
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')
    df['source'] = url
    
    return df

from math import radians, sin, cos, acos
def distance(lat1, lon1, lat2 = 38.870393, lon2=-76.878019):
    try:
        lat1 = radians(float(lat1))
        lon1 = radians(float(lon1))
        lat2 = radians(lat2)
        lon2 = radians(lon2)
        dist = 6371.01 * acos(sin(lat1)*sin(lat2) + cos(lat1)*cos(lat2)*cos(lon1 - lon2))
    #convert from km to m
        dist = dist * 0.621371
    except:
        dist = np.nan
    return dist

def get_comps(homes):
    meta = pd.DataFrame()
    comps = pd.DataFrame()
    for r in tqdm(range(len(homes))):
        address = homes.iloc[r]['address']
        #if pd.to_numeric(df.iloc[r]['living_area']) >0: continue
        #tmp = find_comps(address)
        
        try: 
            tmp = sdat.property_metadata(address)
        except:
            tmp=pd.DataFrame()
        if not tmp.empty:
            tmp['input_string'] = address
            #tmp[0]['median_price'] = tmp[1]['price'].median()
            #tmp[0]['max_price'] = tmp[1]['price'].max()
            #tmp[1]['input_string']  = address
            meta = pd.concat([meta, tmp],join='outer', ignore_index=True, sort =False)
            #comps = pd.concat([comps, tmp[1]])

    homes = pd.merge(left=homes, right = meta,left_on='address', right_on='input_string', how='left', suffixes=("_search",""))
    homes['dist_kj'] = homes.apply(lambda x: distance(x.lat, x.lon), axis=1)


    try:
        leads = pd.read_csv("data/leads.csv")
    except:
        leads = pd.DataFrame()
    homes = pd.concat([leads, homes],join='outer', ignore_index=True, sort =False)
    try:
        print(sum(homes.address_search.duplicated()))
        homes = homes.drop_duplicates(subset=['address_search', 'sale_time'], keep='last')
    except:
        print("error in drop duplicates")
    fName = "data/leads.csv"
    homes.to_csv(fName, index=False)    
    return homes


if __name__ == "__main__":
    redfin = redfin()
    hw = auction_hw()
    ac = auction_ac()
    homes = pd.concat([redfin,hw, ac],ignore_index=True, sort =False)
    #homes = redfin
    #homes['living_area'] = np.nan

    homes = homes.query("property_type !='Townhouse' & property_type != 'Condo/Co-op' & property_type != 'Vacant Land' & property_type != 'Multi-Family (2-4 Unit)'").reset_index()
    meta = sdat.sdat_query(where=sdat.where_meta(homes.address))
    print("got meta 1")
    meta2 = get_comps(homes)
