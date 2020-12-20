#Get data
import pandas as pd
import difflib
pd.options.display.max_columns = 500
import requests
import urllib
import os
from math import radians, sin, cos, acos

def distance(lat1, lon1, lat2 = 38.870393, lon2=-76.878019):
    try:
        lat1 = radians(lat1)
        lon1 = radians(lon1)
        lat2 = radians(lat2)
        lon2 = radians(lon2)
        dist = 6371.01 * acos(sin(lat1)*sin(lat2) + cos(lat1)*cos(lat2)*cos(lon1 - lon2))
    #convert from km to m
        dist = dist * 0.621371
    except:
        dist = None
    return dist


url_md = "opendata.maryland.gov"
#id_md = "pwpp-tk9r"
id_md ="ed4q-f8tm"
app_id = "fuixaI8OmedLW38WcI00DnoCn"

api_csv = "https://opendata.maryland.gov/resource/ed4q-f8tm.csv?"
location = os.path.dirname(os.path.realpath(__file__))
def get_zips():
    
    fName = os.path.join(location, 'data', 'superzip.csv')
    zips = pd.read_csv(fName).query("state=='MD'")
    zips = zips.assign(college = zips.college.str.replace("%", "").map(float))[["zipcode", "centile", "superzip", "rank","adultpop", "households", "college", "income"]]

    return zips

zips = get_zips()

def select_vars(vars =pd.DataFrame() ):
    if vars.empty:
        
        fName = os.path.join(location, 'data', 'vars.csv')
        vars = pd.read_csv(fName)
    vars = vars.dropna().assign(sel = vars.api_field + " as " + vars.short_name)[["sel"]]
    statement = "SELECT " + ' , '.join(vars.sel)

    return statement


def where_comps(lat=38.8159, lon=-76.7497, miles = 1, year = 2019):
    miles = miles/0.000621371 #convert to meters
   
    w = f""" WHERE 
        
        sales_segment_1_transfer_date_yyyy_mm_dd_mdp_field_tradate_sdat_field_89 >= {"'" + str(year) +"%'"} AND 
        sales_segment_1_consideration_mdp_field_considr1_sdat_field_90 > 10000 AND 
        within_circle(mappable_latitude_and_longitude, {lat}, {lon}, {miles})""" #.format(**meta)
    return w.replace("\n", "").replace("  ", "")
        
def where_meta(props):
    if type(props) == str:
        props = [props.replace(",","")]
    if type(props) == pd.Series:
        props = props.to_list()

    if type(props) == list:
        props = ["'" + clean_addresses(prop.replace("'","")) + "'" for prop in props]

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

def sdat_query(api=api_csv, select=select_vars(), where="",limit = 50000,return_page=0):
    query = " ".join(["$query=" ,select , where , "LIMIT" , str(limit)])
    full_url = api + query
    #full_url = URLencode(full_url)
    page = requests.get(full_url)
    if return_page>0:
        return page
    else:
        frame = pd.read_csv(page.url)

    page.close()
    return frame


def property_metadata(address, save_csv = False, select = select_statement):
    '''
        Query SDAT
        Return dictionary with metadata
    '''
    #client = Socrata(url_md, app_id)
    #address = clean_addresses(address)
    #results = client.get(id_md, 
    #                 select =select,
    #                 where ="mdp_street_address_mdp_field_address='{}'".format(address))

    #df = pd.DataFrame(results)


    df = sdat_query(where=where_meta(address))
    
    closest = difflib.get_close_matches(address.upper(), df.address, n=1)
    #print('Closest Property:', closest[0])
    df = df[df.address == closest[0]]
    #df['lon'] = df.location.apply(lambda x: x['coordinates'][0])
    #df['lat'] = df.location.apply(lambda x: x['coordinates'][1])
    #client.close()
    if save_csv:
            sdat = pd.read_csv('sdat_properties.csv')
            #sdat = pd.concat([sdat, df], ignore_index=True)
            result = pd.concat([sdat, df])
            result.to_csv('sdat_properties.csv', index=False)
    return df            

def miles_to_meter(miles):
    return float(miles /0.000621371)

def get_params(df, miles = 1):
    d = dict()
    d['miles'] = miles_to_meter(miles)
    for col in df.columns:
        d[col] = df[col].iloc[0]
    return d

def sdat_comps(address=None, df=pd.DataFrame(), miles=1, year=2019, low_sqft=.9, high_sqft=1.1, *args):
    if df.empty: df = sdat_query(where=where_meta(address))
    comps = sdat_query(where=where_comps(lat = df.lat[0], lon=df.lon[0],miles=miles, year=year))
    land_use = "'" + df.land_use[0] + "'"
    comps = comps.query(f"land_use == {land_use}")
    return comps

def get_query(df, miles =1):
    q_params = get_params(df, miles)
    query = 'SELECT ' + select_statement +  '''
    
    WHERE  
        sales_segment_1_consideration_mdp_field_considr1_sdat_field_90 > 0 AND
        sales_segment_2_consideration_sdat_field_110 > 0 AND
        within_circle(latitude_longitude, {lat}, {lon}, {miles})


    LIMIT 10000
    '''.format(**q_params)
    return query



def add_features(df, dc =dict(lat=38.9072, lon=-77.036)):
    def date_delta(x):
        try:
            res = (pd.to_datetime(x.sale_date) - pd.to_datetime(x.sale_date2))
            
        except:
            res = None
        return res    
        
    try:
        df = df.merge(zips, how='left', on="zipcode")
        df = df.assign(basement = df["style"].str.contains("with basement", case=False),
                  stories = df["style"].str.replace(".*STRY *(.*?) *Story.*", "\\1"),
                  year_built = df.year_built.map(int),
                  age = pd.DatetimeIndex(pd.to_datetime(df.sale_date)).year - df.year_built.map(int) ,
                  acre = df.land_area / df.land_area_unit.str.replace("A", "1").str.replace("S", "43560").map(int),
                  price_delta = df.price - df.price2,
                  date_delta = df.apply(date_delta, axis=1).dt.days / 365,
                  
                  factor_commercial = df.factor_commercial.str.contains("commerical", case=False),
                  yearSold = pd.DatetimeIndex(pd.to_datetime(df.sale_date)).year,
                  monthSold = pd.DatetimeIndex(pd.to_datetime(df.sale_date)).month,
                  isCompany = df.seller.str.contains("INC|LLC|BUILDER", case=False),
                  isBank = df.seller.str.contains("BANK|MORTGAGE|MORTG|SAVING|SECRETARY", case=False)
                  )
        df["isNewConstr"] = (df.yearSold - df.year_built) <= 1
        df["flip"] = (df.price_delta > 50000) & (df.date_delta <=1) & (df.price2 > 10000)
        df["qulity_zipcode"] = .8
        df["dist_dc"] = df.apply(lambda x: distance(x.lat, x.lon,dc["lat"], dc["lon"]), axis=1)
        df["dist_kj"] = df.apply(lambda x: distance(x.lat, x.lon), axis=1)
    except:
        print("error adding features")
        
    return df
                  

def filter_comps(df, price = 75000, turn_around = 400, t_time = pd.to_datetime('2018-01-01')):
    df_copy = df[
    (df['price_change']>=price) & 
    (df['date_change']<=turn_around) & 
    (df.date > pd.to_datetime(t_time))
    ]
    return df_copy


def find_comps(prop, update=True):
    if update:
        done_comps = []
    else:
        comps_df =  pd.read_csv('comps.csv')
        done_comps = comps_df.id.unique()        
    try:
        meta, comp = get_comps_sdat(prop)
        if comp.id.iloc[0] in done_comps:
            #do nothing
            print(prop, 'already done')
            filt_comp = filter_comps(comps_df.query('id == {}'.format(comps.id.iloc[0])))
            
        else:
            #load comp to csv
            filt_comp = filter_comps(comp)
            #filt_comp.to_csv('comps.csv', mode="a", header=False, index = False)

    except:
        search_address = prop.split()[:-1]
        search_address = ' '.join(search_address)
        try:
            if comp.id.iloc[0] in done_comps:
                #do nothing
                print(prop, 'already done')
            else:
                meta, comp = get_comps_sdat(search_address)
                filt_comp = filter_comps(comp, price=50000)
                #filt_comp.to_csv('comps.csv', mode="a", header=False, index = False)
        except:
            print('{}: Could not find property'.format(prop))
            return None
    
    results = (meta, filt_comp)    
    return results #filt_comp

def potential_property(f=None, df=None, address_col='address'):
    '''
    Function will create comps for each property in csv
    
    PARAMS:
        f: filename
        df: optional parameter to provide dataframe
        address_col: name of column that contains address
    '''
    if isinstance(df, pd.DataFrame): pass
    else: df = pd.read_csv(f)
    df.columns = df.columns.str.lower()
    address_col = address_col.lower()
    comps = pd.read_csv('comps.csv')
    done_comps = [] #comps.id.unique()
    
    properties = pd.read_csv('properties.csv')
    
    
    
    for prop in df[address_col].unique():
        comps = find_comps(prop)
        if isinstance(comps, pd.DataFrame):
            #do something
            vals = {'Address': [comps.id.iloc[0]],
                    'Re-Sale Price': [comps['price'].median()], 
                    'List Price': [df[df['address'] == prop]['current price'].mean()] 
                   }
            tmp_prop = pd.DataFrame(vals)
            tmp_prop['Margin'] = tmp_prop['Re-Sale Price'] - tmp_prop['List Price']
            properties = properties.append(tmp_prop, ignore_index=True)
            tmp_prop = None
        else:
            print(prop, 'could not be added')
        comps = None
    return properties
