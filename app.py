from flask import Flask, Response, request, make_response
from geojson import dumps as geojson_dump
from pull_nwis import pull_nwis_data_stream, pull_nwis_data_generator, pull_nwis_data_generator_multiple_hucs
from requests import head


app = Flask(__name__)


def generate_geojson_from_list(feature_list):
    """
    based on https://blog.al4.co.nz/2016/01/streaming-json-with-flask/
    A lagging generator to stream JSON so we don't have to hold everything in memory
    This is a little tricky, as we need to omit the last comma to make valid JSON,
    thus we use a lagging generator, similar to http://stackoverflow.com/questions/1630320/
    """
    features = iter(feature_list)
    try:
        prev_feature = next(features)  # get first result
    except StopIteration:
        # StopIteration here means the length was zero, so yield a valid releases doc and stop
        yield '{"features": []}'
        raise StopIteration
    # We have some features. First, yield the opening json for geojson
    yield '{"crs":{"type": "name","properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},"type": "FeatureCollection","features": ['
    # Iterate over the releases
    for feature in features:
        yield geojson_dump(prev_feature) + ', '
        prev_feature = feature
    # Now yield the last iteration without comma but with the closing brackets
    yield geojson_dump(prev_feature) + ']}'


def generate_geojson_from_generator(params, huc_list=None):
    """
    based on https://blog.al4.co.nz/2016/01/streaming-json-with-flask/
    A lagging generator to stream JSON so we don't have to hold everything in memory
    This is a little tricky, as we need to omit the last comma to make valid JSON,
    thus we use a lagging generator, similar to http://stackoverflow.com/questions/1630320/
    The lagging generator is fed by
    """
    if huc_list:
        features = pull_nwis_data_generator_multiple_hucs(huc_list, params)
    else:
        features = pull_nwis_data_generator(params)
    try:
        prev_feature = next(features)  # get first result
    except StopIteration:
        # StopIteration here means the length was zero, so yield a valid releases doc and stop
        yield '{"features": []}'
        raise StopIteration
    # We have some features. First, yield the opening json for geojson
    yield '{"crs":{"type": "name","properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},"type": "FeatureCollection","features": ['
    # Iterate over the releases
    for feature in features:
        yield geojson_dump(prev_feature) + ', '
        prev_feature = feature
    # Now yield the last iteration without comma but with the closing brackets
    yield geojson_dump(prev_feature) + ']}'





@app.route("/")
def hello():
    return "Hello World!"

@app.route("/poc/")
def poc():
    feature_list = pull_nwis_data_stream(['03050201', '03180004'])
    return Response(generate_geojson_from_list(feature_list), content_type='application/json')

@app.route("/poc_stream/")
def poc_stream():
    return Response(generate_geojson_from_generator(['01', '02', '03']), content_type='application/json')

@app.route("/sites/")
def sites():
    args = dict(request.args)
    args['format'] = 'rdb'
    major_arguments_list = ['sites','stateCD','huc','bBox','countyCd']
    if any(k in major_arguments_list for k in args.iterkeys()):
        nwis_head = head('http://waterservices.usgs.gov/nwis/site/', params=args)
        if nwis_head.status_code == 200:
            return Response(generate_geojson_from_generator(args), content_type='application/json')
        elif nwis_head.status_code == 400 and ('Major HUC list size exceeded' in nwis_head.reason or 'Minor HUC list size exceeded' in nwis_head.reason):
            huc_string = args['huc'][0]
            small_huc_list = huc_string.split(',')
            del args['huc']
            return Response(generate_geojson_from_generator(args, small_huc_list), content_type='application/json')



        else:
            resp = make_response(nwis_head.reason, nwis_head.status_code)
            resp.headers["X-Error-Reason"] = nwis_head.reason
            return resp
    else:
        huc_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16',
                    '17', '18', '19', '20', '21']
        return Response(generate_geojson_from_generator(args, huc_list), content_type='application/json')


if __name__ == "__main__":
    app.run(debug=True)