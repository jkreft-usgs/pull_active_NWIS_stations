from flask import Flask, Response, request, make_response
from requests import head
from utils import generate_geojson_from_generator

app = Flask(__name__)


@app.route("/")
def hello():
    return "Hello World!"


@app.route("/sites/")
def sites():
    """
    This endpoint is meant to essentially emulate the NWIS sites API, with the notable exception that there is no
    restriction on the number of hucs that can be requested, and that the data that are produced are in the web-friendly
    geoJSON format
    :return: a geojson feature collection
    """
    args = dict(request.args)
    args['format'] = 'rdb'
    major_arguments_list = ['sites', 'stateCD', 'huc', 'bBox', 'countyCd']
    # do this test so that we can take advantage of the NWIS site service validations
    if any(k in major_arguments_list for k in args.iterkeys()):
        nwis_head = head('http://waterservices.usgs.gov/nwis/site/', params=args)
        if nwis_head.status_code == 200:
            return Response(generate_geojson_from_generator(args), content_type='application/json')
        elif nwis_head.status_code == 400 and \
                ('Major HUC list size exceeded' in nwis_head.reason or 'Minor HUC list size exceeded' in nwis_head.reason):
            huc_string = args['huc'][0]
            small_huc_list = huc_string.split(',')
            del args['huc']
            return Response(generate_geojson_from_generator(args, small_huc_list), content_type='application/json')
        else:
            resp = make_response(nwis_head.reason, nwis_head.status_code)
            resp.headers["X-Error-Reason"] = nwis_head.reason
            return resp
    else:
        # OK, we are gonna go for the whole country, hang onto your hats
        huc_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16',
                    '17', '18', '19', '20', '21']
        return Response(generate_geojson_from_generator(args, huc_list), content_type='application/json')


if __name__ == "__main__":
    app.run(debug=True)
