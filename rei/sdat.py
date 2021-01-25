# Get data
from sklearn.neighbors import NearestNeighbors
import statsmodels.formula.api as smf

from math import radians, sin, cos, acos
import os
import urllib
import requests
import numpy as np
import pandas as pd
import difflib
pd.options.display.max_columns = 500


def distance(lat1, lon1, lat2=38.870393, lon2=-76.878019):
    try:
        lat1 = radians(lat1)
        lon1 = radians(lon1)
        lat2 = radians(lat2)
        lon2 = radians(lon2)
        dist = 6371.01 * acos(sin(lat1)*sin(lat2) +
                              cos(lat1)*cos(lat2)*cos(lon1 - lon2))
    # convert from km to m
        dist = dist * 0.621371
    except:
        dist = None
    return dist


url_md = "opendata.maryland.gov"
#id_md = "pwpp-tk9r"
id_md = "ed4q-f8tm"
app_id = "fuixaI8OmedLW38WcI00DnoCn"

api_csv = "https://opendata.maryland.gov/resource/ed4q-f8tm.csv?"
location = os.path.dirname(os.path.realpath(__file__))


def get_zips():

    fName = os.path.join(location, 'data', 'superzip.csv')
    zips = pd.read_csv(fName).query("state=='MD'")
    zips = zips.assign(college=zips.college.str.replace("%", "").map(float))[
        ["zipcode", "centile", "superzip", "rank", "adultpop", "households", "college", "income"]]

    return zips


zips = get_zips()


def select_vars(vars=pd.DataFrame()):
    if vars.empty:

        fName = os.path.join(location, 'data', 'vars.csv')
        vars = pd.read_csv(fName)
    vars = vars.dropna().assign(sel=vars.api_field +
                                " as " + vars.short_name)[["sel"]]
    statement = "SELECT " + ' , '.join(vars.sel)

    return statement


def where_comps(lat=38.8159, lon=-76.7497, miles=1, year=2019, land_use=["Residential (R)"], **kwargs):
    miles = miles/0.000621371  # convert to meters
    
    w = f""" WHERE 
        
        sales_segment_1_transfer_date_yyyy_mm_dd_mdp_field_tradate_sdat_field_89 >= {"'" + str(year) +"%'"} AND 
        sales_segment_1_consideration_mdp_field_considr1_sdat_field_90 > 10000 AND 
        within_circle(mappable_latitude_and_longitude, {lat}, {lon}, {miles})"""

    # construct land_use    
    if type(land_use) == str: land_use = [land_use]
    land_use = "(" + ",".join(["'"+ i + "'" for i in  land_use]) + ")"
    
    if len(land_use) > 0 :
        w = w  + f""" AND land_use_code_mdp_field_lu_desclu_sdat_field_50 IN {land_use}"""

    for key, value in kwargs.items():
        w = w + " AND " + key + str(value)

    return w.replace("\n", "").replace("  ", "")


def where_meta(props):
    if type(props) == str:
        props = [props.replace(",", "")]
    if type(props) == pd.Series:
        props = props.to_list()

    if type(props) == list:
        props = [
            "'" + clean_addresses(prop.replace("'", "")) + "'" for prop in props]

    props_str = ",".join(props)

    w = f" WHERE mdp_street_address_mdp_field_address in ({props_str}) "

    return w


def clean_addresses(addresses):

    props_str = addresses.upper()
    props_str = props_str.replace("STREET", "ST")
    props_str = props_str.replace("AVENUE", "AVE")
    props_str = props_str.replace("DRIVE", "DR")
    props_str = props_str.replace("LN", "LANE")
    props_str = props_str.replace("TERRACE", "TER")
    props_str = props_str.replace("COURT", "CT")
    props_str = props_str.replace(" RD", " ROAD")
    props_str = props_str.replace("TRAIL", "TRL")
    props_str = props_str.replace("PLACE", "PL")
    props_str = props_str.replace("BOULEVARD", "BLVD")

    return props_str


select_statement = select_vars()


def sdat_query(api=api_csv, select=select_vars(), where="", limit=50000, return_page=0):
    query = " ".join(["$query=", select, where, "LIMIT", str(limit)])
    full_url = api + query
    #full_url = URLencode(full_url)
    page = requests.get(full_url)
    if return_page > 0:
        return page
    else:
        frame = pd.read_csv(page.url)

    page.close()
    return frame


def property_metadata(address, save_csv=False, select=select_statement):
    '''
        Query SDAT
        Return dictionary with metadata
    '''
    #client = Socrata(url_md, app_id)
    #address = clean_addresses(address)
    # results = client.get(id_md,
    #                 select =select,
    #                 where ="mdp_street_address_mdp_field_address='{}'".format(address))

    #df = pd.DataFrame(results)

    df = sdat_query(where=where_meta(address))

    closest = difflib.get_close_matches(address.upper(), df.address, n=1)
    #print('Closest Property:', closest[0])
    df = df[df.address == closest[0]]
    #df['lon'] = df.location.apply(lambda x: x['coordinates'][0])
    #df['lat'] = df.location.apply(lambda x: x['coordinates'][1])
    # client.close()
    if save_csv:
        sdat = pd.read_csv('sdat_properties.csv')
        #sdat = pd.concat([sdat, df], ignore_index=True)
        result = pd.concat([sdat, df])
        result.to_csv('sdat_properties.csv', index=False)
    return df


def miles_to_meter(miles):
    return float(miles / 0.000621371)


def get_params(df, miles=1):
    d = dict()
    d['miles'] = miles_to_meter(miles)
    for col in df.columns:
        d[col] = df[col].iloc[0]
    return d


def sdat_meta(address):
    df = sdat_query(where=where_meta(address))
    df = add_features(df)
    return df


def sdat_comps(address=None, df=pd.DataFrame(), miles=1, year=2020, low_sqft=.9, high_sqft=1.1, *args):
    if df.empty: df = sdat_query(where=where_meta(address))
    comps = sdat_query(
        where=where_comps(
            lat=df.lat[0], 
            lon=df.lon[0], 
            miles=miles, 
            year=year, 
            land_use=df.land_use[0]
            )
        )
 
    comps = add_features(comps)
    comps = comps.assign(
        dist_comp = comps.apply(lambda x: distance(x.lat, x.lon, df["lat"][0], df["lon"][0]), axis=1)
    )
    return comps


def get_query(df, miles=1):
    q_params = get_params(df, miles)
    query = 'SELECT ' + select_statement + '''
    
    WHERE  
        sales_segment_1_consideration_mdp_field_considr1_sdat_field_90 > 0 AND
        sales_segment_2_consideration_sdat_field_110 > 0 AND
        within_circle(latitude_longitude, {lat}, {lon}, {miles})
    LIMIT 10000
    '''.format(**q_params)
    return query


def add_features(df, dc=dict(lat=38.9072, lon=-77.036)):

    def acre(df):
        try:
            res = df.land_area_unit.str.replace(
                "S", "1").str.replace("A", "43560")
            res = pd.to_numeric(res, errors="coerce")
            res = res * df.land_area
        except:
            res = None
        return res

    def date_delta(x):
        try:
            res = pd.to_datetime(x.sale_date) - pd.to_datetime(x.sale_date2)
            res = res.dt.days / 365

        except:
            res = None
        return res

    try:
        df = df.merge(zips, how='left', on="zipcode")
        df = df.assign(sale_date=pd.to_datetime(df.sale_date, errors='coerce'),
                       sale_date2=pd.to_datetime(
                           df.sale_date2, errors='coerce')

                       )
        df = df.assign(basement=df["style"].str.contains("with basement", case=False),
                       stories=df["style"].str.replace(
                           ".*STRY *(.*?) *Story.*", "\\1").str.replace("TH |Center|End|STRY|", "", case=False),
                       year_built=df.year_built.map(int),
                       age=df.sale_date.dt.year - df.year_built.map(int),
                       acre=acre(df),
                       price_delta=df.price - df.price2,
                       date_delta=df.apply(date_delta, axis=1),

                       factor_commercial=df.factor_commercial.str.contains(
                           "commerical", case=False),
                       yearSold=df.sale_date.dt.year,
                       monthSold=df.sale_date.dt.month,
                       isCompany=df.seller.str.contains(
                           "INC|LLC|BUILDER", case=False),
                       isBank=df.seller.str.contains(
                           "BANK|MORTGAGE|MORTG|SAVING|SECRETARY", case=False)
                       )
        df["isNewConstr"] = (df.yearSold - df.year_built) <= 1
        df["flip"] = (df.price_delta > 50000) & (
            df.date_delta <= 1) & (df.price2 > 10000)
        df["qulity_zipcode"] = .8
        df["dist_dc"] = df.apply(lambda x: distance(
            x.lat, x.lon, dc["lat"], dc["lon"]), axis=1)
        df["dist_kj"] = df.apply(lambda x: distance(x.lat, x.lon), axis=1)
    except:
        print("error adding features")

    return df


class Home:
    view_basic = ["address", "price", "sqft", "acre"]
    view_basic_analyze = ["price", "sqft", "acre", "basement", "stories"]
    #view_analyze = [col for col in df.columns]

    def __init__(self, address):
        self.address = address
        self.meta = sdat_meta(address)
        self.comps = sdat_comps(df=self.meta)
        self.comps = self.comps.assign(dist_address=self.comps.apply(lambda x: distance(
            x.lat, x.lon, self.meta["lat"][0], self.meta["lon"][0]), axis=1))
        self.comps_filtered = self.filter_comps()
        self.comps_knn = self.knn()
        self.arv = self.arv()

    def filter_comps(self, low_sqft=.8, high_sqft=1.2, price=75000, turn_around=400, t_time=pd.to_datetime('2018-01-01')):
        
        if low_sqft < 10: low_sqft = self.meta.sqft[0] * low_sqft
        if high_sqft < 10: high_sqft = self.meta.sqft[0] * high_sqft

        df = self.comps.copy()
        df_copy = df[
            (
                #(df.price_delta >= price) &
                #(df['date_delta'] <= turn_around) &
                #(pd.to_datetime(df["sale_date"]) > pd.to_datetime(t_time)) &
                (df.sqft > int(self.meta.sqft[0] * .8)) & 
                (df.sqft < int(self.meta.sqft[0] * 1.2)) &
                (df.isBank == False) & 
                #((df.isCompany == True) | (df.flip == True)) &
                (df.basement == self.meta.basement[0])
            )
        ]
        return df_copy

    def arv(self):
        return self.comps_filtered.price.describe().reset_index()

    def knn(self):

        return 1

    def ols(self):
        self.lm = smf.ols('price~sqft + basement', data=self.comps).fit()
        pred = self.lm.predict(self.meta)
        return pred

    def rf():

        return 1


class Homes():
    #__home__ = Home("1303 Alberta Dr")
    #__members__ = [attr for attr in dir(__home__) if not callable(getattr(__home__, attr)) and not attr.startswith("__")]

    def __init__(self, addresses):
        self.add(addresses)
        self.addresses = addresses
        self.homes_dict = {adr: Home(adr) for adr in addresses}
        self.meta_all = pd.concat(
            [self.homes_dict[adr].meta for adr in addresses])
        self.comps_all = pd.concat(
            [self.homes_dict[adr].comps.assign(Home=adr) for adr in addresses])
        self.comps_filtered = pd.concat(
            [self.homes_dict[adr].comps_filtered.assign(Home=adr) for adr in addresses])

    def view(self):
        prices = self.comps_filtered.groupby(
            "Home").price.describe().reset_index()
        prices = prices.assign(address=prices.Home.apply(clean_addresses))
        prices.drop(columns=['Home'], inplace=True)
        df = self.meta_all[['address', 'sqft']].merge(
            prices, how='left', on='address')
        return df

    def add(self,addresses):
        

        if type(addresses) == str: addresses = [addresses]
        for adr in addresses:
            
            self.homes_dict[adr] = Home(adr)
            


if __name__ == '__main__':
    #h = Home('1303 Alberta Dr')
    hs = Homes(["1303 Alberta Dr", "1406 Iron Forge Rd"])
    print(hs.meta_all)
