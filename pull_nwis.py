from requests import Session
from cStringIO import StringIO
import tablib
from geojson import Feature, Point, FeatureCollection, dumps as geojson_dump
from copy import deepcopy
import json
from pyproj import Proj, transform

big_huc_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17',
                '18', '19', '20', '21']

two_8_digit_hucs = ['03050201', '03180004']

one_two_digit_huc = ['03']

three_2_digit_hucs = ['01', '02', '03']


site_types_dict = {"ES": "Estuary",
                   "LK": "Lake, Reservoir, Impoundment",
                   "OC": "Ocean",
                   "OC-CO": "Coastal",
                   "ST": "Stream",
                   "ST-CA": "Canal",
                   "ST-DCH": "Ditch",
                   "ST-TS": "Tidal stream",
                   "WE": "Wetland"}


def process_nwis_data(content):
    dataset = tablib.Dataset()
    comments = True
    definitions = True
    for line in content.readlines():
        li = line.strip()
        if li.startswith("#"): # lets ignore comments
            pass
        elif comments and not (li.startswith("#")):  # first row after the comments in the rdb are column headings
            comments = False
            headers = li.split('\t')
            dataset.headers = headers
        elif comments == False and definitions == True:  # ok now we need to ignore the useless data definitions
            definitions = False
        elif comments == False and definitions == False:  # finally we are adding data together
            row = li.split('\t')
            dataset.append(row)
    return dataset


def build_sites_geojson(sites_data, site_types):
    feature_list = []
    for station in sites_data.dict:
        try:
            station_lat = float(station['dec_lat_va'])
            station_long = float(station['dec_long_va'])
            if station['dec_coord_datum_cd'] == 'NAD83':
                p1 = Proj(init='epsg:26912')
                p2 = Proj(init='epsg:4326')
                x1, y1 = p1(float(station['dec_long_va']), float(station['dec_lat_va']))
                x2, y2 = transform(p1, p2, x1, y1)
                feature = Feature(geometry=Point((x2, y2)),
                                  properties={"stationName": station['station_nm'],
                                              "agencyCode": station['agency_cd'],
                                              "siteNumber": station['site_no'],
                                              "hucCode": station['huc_cd'],
                                              "SiteTypeCode": station['site_tp_cd'],
                                              "SiteType": site_types.get(station['site_tp_cd']),
                                              "url": 'http://waterdata.usgs.gov/nwis/inventory?agency_code='+station['agency_cd']+'&site_no='+station['site_no']})
                feature_list.append(feature)
            else:
                print('Not NAD83!!!! '+station['site_no']+' is '+station['dec_coord_datum_cd'])
                print(station)
                print(
                    'url: http://waterdata.usgs.gov/nwis/inventory?agency_code=' + station['agency_cd'] + '&site_no=' +
                    station[
                        'site_no'])
        except ValueError:
            print('ValueError!')
            print(station)
            print(
            'url: http://waterdata.usgs.gov/nwis/inventory?agency_code=' + station['agency_cd'] + '&site_no=' + station[
                'site_no'])
    return feature_list

def pull_nwis_data(huc_list):
    """

    :param huc_list:
    :return:
    """
    base_params = {'format': 'rdb',
                   'siteType': 'ST',
                   'siteStatus': 'active',
                   'hasDataTypeCd': 'iv,dv',
                   }
    s = Session()
    total_site_list = []
    for huc in huc_list:
        params = deepcopy(base_params)
        params['huc'] = huc
        r = s.get('http://waterservices.usgs.gov/nwis/site/', params=params,
                  headers={'Accept-Encoding': 'gzip,deflate'})
        if r.status_code == 200:
            f = StringIO(r.content)
            dataset = process_nwis_data(f)
            huc_feature_list = build_sites_geojson(dataset, site_types_dict)
            total_site_list.extend(huc_feature_list)
        print('finished huc '+huc)

    return total_site_list


def build_feature_collection(huc_list):
    nwis_feature_list = pull_nwis_data(huc_list)
    station_collection = FeatureCollection(nwis_feature_list, crs={"properties": {"name": "urn:ogc:def:crs:EPSG::4326"},
                                                                   "type": "name"})
    return station_collection


geojson_item = build_feature_collection(big_huc_list)

content = geojson_dump(geojson_item)
json_content = json.loads(content)
with open('data.txt', 'w') as outfile:
    json.dump(json_content, outfile, sort_keys=False, indent=4, separators=(',', ': '))

print('success!')


