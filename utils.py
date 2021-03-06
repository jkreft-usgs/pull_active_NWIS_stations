from requests import Session, get
import tablib
from geojson import Feature, Point, dumps as geojson_dump
from pyproj import Proj, transform


# This huge dict lets us map the site type codes to actual, non-expert-useful values.
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


def build_site_feature(station, site_types):
    """
    This takes a single row of dictionary output from a tablib ordered dict that was generated from a rdb file, and
    returns a python-encoded geojson feature object that can be used either directly, or as an input to a function that
    builds a feature collection
    :param station: a dictionary with keys that map to the standard site service rdb column headings
    :param site_types: a dictionary of NWIS site type codes and their actual meanings.
    :return: a python-encoded geojson feature object, with the lat-long translated to EPSG:4326
    """
    try:
        if station['dec_coord_datum_cd'] == 'NAD83':  # 99.99 of site data is generated as NAD83
            # here we need to translate from NAD83 to web mercator, which is the geoJSON default
            p1 = Proj(init='epsg:26912')
            p2 = Proj(init='epsg:4326')
            x1, y1 = p1(float(station['dec_long_va']), float(station['dec_lat_va']))
            x2, y2 = transform(p1, p2, x1, y1)
            # standard pygeoJSON feature generation
            feature = Feature(geometry=Point((x2, y2)),
                              properties={"stationName": station['station_nm'],
                                          "agencyCode": station['agency_cd'],
                                          "siteNumber": station['site_no'],
                                          "hucCode": station.get('huc_cd'),
                                          "SiteTypeCode": station['site_tp_cd'],
                                          "SiteType": site_types.get(station['site_tp_cd']),
                                          "url": 'http://waterdata.usgs.gov/nwis/inventory?agency_code=' + station[
                                              'agency_cd'] + '&site_no=' + station['site_no']})

            return feature
        else:  # we are lazily only supporting NAD83 output from the site service.
            # TODO: dump this into logging
            print('Not NAD83!!!! ' + station['site_no'] + ' is ' + station['dec_coord_datum_cd'])
            print(station)
            print(
                'url: http://waterdata.usgs.gov/nwis/inventory?agency_code=' + station['agency_cd'] + '&site_no=' +
                station[
                    'site_no'])
            return None
    except ValueError:  # this catches sites that are not populated with essential features (coordinates, etc)
        # TODO: dump this into logging
        print('ValueError!')
        print(station)
        print('url: http://waterdata.usgs.gov/nwis/inventory?agency_code=' + station['agency_cd'] +
              '&site_no=' + station['site_no'])
        return None

# TODO: combine pull_nwis_data_generator and pull_nwis_data_generator_multiple_hucs


def pull_nwis_data_generator(params):
    """
    This iterable function (aka generator) pulls NWIS site rdb data for a single call, strips the useless stuff out,
    and then row-by-row it kicks out a python-encoded geojson feature object to feed something else that needs the
    features, such as a function that generates a feature collection
    :param params: the parameter dictionary to send to the NWIS site service
    """
    r = get('http://waterservices.usgs.gov/nwis/site/',
            params=params, headers={'Accept-Encoding': 'gzip,deflate'}, stream=True)
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
            elif comments is False and definitions is True:  # ok now we need to ignore the useless data definitions
                definitions = False
            elif comments is False and definitions is False:  # finally we are adding data together
                row = li.split('\t')
                dataset.append(row)
                feature = build_site_feature(dataset.dict[0], site_types_dict)
                if feature:
                    yield feature
                dataset.pop()


def pull_nwis_data_generator_multiple_hucs(huc_list, params):
    """
    This generator is really similar to pull_nwis_data_generator, but it makes multiple calls to the NWIS site service
    to make it possible to yield national scale datasets. It calls, huc-by-huc, feeding the data into a downstream
    generator
    :param huc_list: list of hucs to chug through
    :param params: all the other params to send to the NWIS site service
    :return:
    """
    s = Session()
    for huc in huc_list:
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
                elif comments and not (li.startswith("#")):  # first row after the comments are column headings
                    comments = False
                    headers = li.split('\t')
                    dataset.headers = headers
                elif comments is False and definitions is True:  # ok now we need to ignore the useless data definitions
                    definitions = False
                elif comments is False and definitions is False:  # finally we are adding data together
                    row = li.split('\t')
                    dataset.append(row)
                    feature = build_site_feature(dataset.dict[0], site_types_dict)
                    if feature:
                        yield feature
                    dataset.pop()
            print('finished huc ' + huc)


def generate_geojson_from_generator(params, huc_list=None):
    """
    based on https://blog.al4.co.nz/2016/01/streaming-json-with-flask/
    A lagging generator to stream JSON so we don't have to hold everything in memory
    This is a little tricky, as we need to omit the last comma to make valid JSON,
    thus we use a lagging generator, similar to http://stackoverflow.com/questions/1630320/
    The lagging generator is fed by one of two generators, depending on if there is going to be a single call to the
    NWIS site service, or a huc-by-huc call for national or larger scale data.
    """
    if huc_list:
        features = pull_nwis_data_generator_multiple_hucs(huc_list, params)
    else:
        features = pull_nwis_data_generator(params)
    try:
        prev_feature = next(features)  # get first result
    except StopIteration:
        # StopIteration here means the length was zero, so yield a valid geojson doc and stop
        yield '{"type": "FeatureCollection","features": []}'
        raise StopIteration
    # We have some features. First, yield the opening json for geojson
    yield '{"crs":{"type": "name","properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},' \
          '"type": "FeatureCollection","features": [\n'
    # Iterate over the releases
    for feature in features:
        yield geojson_dump(prev_feature) + ', \n'
        prev_feature = feature
    # Now yield the last iteration without comma but with the closing brackets
    yield geojson_dump(prev_feature) + ']}'
