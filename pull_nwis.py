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

one_8_digit_huc = ['03050201']

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
                   "WE": "Wetland",
                   "GW": "Well",
                   "GW-CR": "Collector or Ranney type well",
                   "GW-EX": "Extensometer well",
                   "GW-HZ": "Hyporheic-zone well",
                   "GW-IW": "Interconnected wells",
                   "GW-MW": "Multiple wells",
                   "GW-TH": "Test hole not completed as a well",
                   "SB": "Subsurface",
                   "SB-CV": "Cave",
                   "SB-GWD": "Groundwater drain",
                   "SB-TSM": "Tunnel, shaft, or mine",
                   "SB-UZ": "Unsaturated zone",
                   "SP": "Spring",
                   "AT": "Atmosphere",
                   "AG": "Aggregate groundwater use",
                   "AS": "Aggregate surface-water-use",
                   "AW": "Aggregate water-use establishment",
                   "FA": "Facility",
                   "FA-AWL": "Animal waste lagoon",
                   "FA-CI": "Cistern",
                   "FA-CS": "Combined sewer",
                   "FA-DV": "Diversion",
                   "FA-FON": "Field, Pasture, Orchard, or Nursery",
                   "FA-GC": "Golf course",
                   "FA-HP": "Hydroelectric plant",
                   "FA-LF": "Landfill",
                   "FA-OF": "Outfall",
                   "FA-PV": "Pavement",
                   "FA-QC": "Laboratory or sample-preparation area",
                   "FA-SEW": "Wastewater sewer",
                   "FA-SPS": "Septic system",
                   "FA-STS": "Storm sewer",
                   "FA-TEP": "Thermoelectric plant",
                   "FA-WDS": "Water-distribution system",
                   "FA-WIW": "Waste injection well",
                   "FA-WTP": "Water-supply treatment plant",
                   "FA-WWD": "Wastewater land application",
                   "FA-WWTP": "Wastewater-treatment plant",
                   "FA-WU": "Water-use establishment",
                   "GL": "Glacier",
                   "LA": "Land",
                   "LA-EX": "Excavation",
                   "LA-OU": "Outcrop",
                   "LA-PLY": "Playa",
                   "LA-SH": "Soil hole",
                   "LA-SNK": "Sinkhole",
                   "LA-SR": "Shore",
                   "LA-VOL": "Volcanic vent"}


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

def build_site_dict(station, site_types):
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
                                      "url": 'http://waterdata.usgs.gov/nwis/inventory?agency_code=' + station[
                                          'agency_cd'] + '&site_no=' + station['site_no']})

        return feature
    else:
        print('Not NAD83!!!! ' + station['site_no'] + ' is ' + station['dec_coord_datum_cd'])
        print(station)
        print(
            'url: http://waterdata.usgs.gov/nwis/inventory?agency_code=' + station['agency_cd'] + '&site_no=' +
            station[
                'site_no'])
        return None

def build_sites_geojson(sites_data, site_types):
    feature_list = []
    for station in sites_data.dict:
        try:
            feature = build_site_dict(station, site_types)
            if feature:
                feature_list.append(feature)
        except ValueError:
            print('ValueError!')
            print(station)
            print(
            'url: http://waterdata.usgs.gov/nwis/inventory?agency_code=' + station['agency_cd'] + '&site_no=' + station[
                'site_no'])
    return feature_list

def pull_nwis_data(huc_list):
    """
    This pulls NWIS site data, one HUC at a time, and returns a list of geojson feature objects
    that can be fed into a feature collection building tool
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

def pull_nwis_data_stream(huc_list):
    """
    This pulls NWIS site data, one HUC at a time, and returns a list of geojson feature objects
    that can be fed into a feature collection building tool.  However, it parses the returned RDB as a stream instead of
    waiting to download the entire file for each HUC.
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
                  headers={'Accept-Encoding': 'gzip,deflate'}, stream=True)
        if r.status_code == 200:
            dataset = tablib.Dataset()
            comments = True
            definitions = True
            for line in r.iter_lines():
                li = line.strip()
                if li.startswith("#"):  # lets ignore comments
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
            huc_feature_list = build_sites_geojson(dataset, site_types_dict)
            total_site_list.extend(huc_feature_list)
        print('finished huc '+huc+' ('+str(r.status_code)+')')

    return total_site_list

def pull_nwis_data_generator(huc_list):
    """
    This pulls NWIS site data, one HUC at a time, and returns a list of geojson feature objects
    that can be fed into a feature collection building tool.  However, it parses the returned RDB as a stream instead of
    waiting to download the entire file for each HUC. It then yields that geojson object one at a time so that
    something else can do something with it
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
                  headers={'Accept-Encoding': 'gzip,deflate'}, stream=True)
        if r.status_code == 200:
            dataset = tablib.Dataset()
            comments = True
            definitions = True
            for line in r.iter_lines():
                li = line.strip()
                if li.startswith("#"):  # lets ignore comments
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
                    feature = build_sites_geojson(dataset, site_types_dict)
                    if feature:
                        yield feature[0]
                    dataset.pop()
        print('finished huc '+huc+' ('+str(r.status_code)+')')



def build_feature_collection(huc_list):
    nwis_feature_list = pull_nwis_data_stream(huc_list)
    station_collection = FeatureCollection(nwis_feature_list, crs={"properties": {"name": "urn:ogc:def:crs:EPSG::4326"},
                                                                   "type": "name"})
    return station_collection


def build_geojson_file(huc_list):
    geojson_item = build_feature_collection(huc_list)
    content = geojson_dump(geojson_item)
    json_content = json.loads(content)
    with open('data.txt', 'w') as outfile:
        json.dump(json_content, outfile, sort_keys=False, indent=4, separators=(',', ': '))
    print('success!')


#pull_nwis_data_stream(one_8_digit_huc)
#build_geojson_file(big_huc_list)
#build_geojson_file(two_8_digit_hucs)