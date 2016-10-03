from flask import Flask, Response, request, make_response
from requests import head
from utils import generate_geojson_from_generator

app = Flask(__name__)


@app.route("/")
def hello():
    """
    I hate not having anything at the root
    :return:
    """
    return "Hello World!"


@app.route("/sites/")
def sites():
    """
    This endpoint is meant to essentially emulate the NWIS sites API, with the notable exception that there is no
    restriction on the number of hucs that can be requested, and that the data that are produced are in the web-friendly
    geoJSON format
    :return: a geojson feature collection
    """
    # request.args is an immutable multidict, so we need to turn it into something we can use.
    # It's a kinda weird thing with each value being a list, but requests knows what to do with it, so that's what we
    # are using.
    args = dict(request.args)
    args['format'] = 'rdb'  # we always want the format to be rdb
    major_arguments_list = ['sites', 'stateCD', 'huc', 'bBox', 'countyCd']  # the NWIS sites service needs one of these
    # do this test so that we can take advantage of the NWIS site service validations
    if any(k in major_arguments_list for k in args.iterkeys()):
        nwis_head = head('http://waterservices.usgs.gov/nwis/site/', params=args)  # do a head request for validation
        if nwis_head.status_code == 200:  # hey, it's cool to do it with a single call, we can rock and roll
            return Response(generate_geojson_from_generator(args), content_type='application/json')
        # there is only allowed 1 HUC2 or 10 HUC8s in the query...and we are allowing as many as you want
        elif nwis_head.status_code == 400 and \
                ('Major HUC list size exceeded' in nwis_head.reason or 'Minor HUC list size exceeded' in nwis_head.reason):
            huc_string = args['huc'][0]  # get the request huc parameter string out of the multi-dict list
            small_huc_list = huc_string.split(',')  # turn comma-delimited huc parameter string into a list
            del args['huc']  # drop the huc parameter from the dict of arguments to send to the NWIS site service
            return Response(generate_geojson_from_generator(args, small_huc_list), content_type='application/json')
        else:
            resp = make_response(nwis_head.reason, nwis_head.status_code)
            resp.headers["X-Error-Reason"] = nwis_head.reason
            return resp
    else:
        # OK, we are gonna go for the whole country, hang onto your hats
        huc_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16',
                    '17', '18', '19', '20', '21']
        # Check to see if there is anything else is wrong with the query
        check_params = args
        check_params['huc'] = huc_list[0]
        nwis_head = head('http://waterservices.usgs.gov/nwis/site/', params=check_params)
        if nwis_head.status_code == 200:
            return Response(generate_geojson_from_generator(args, huc_list), content_type='application/json')
        else:
            resp = make_response(nwis_head.reason, nwis_head.status_code)
            resp.headers["X-Error-Reason"] = nwis_head.reason
            return resp


@app.route("/por_sites/<data_type>/<parameter_code>/")
def por_sites(data_type, parameter_code):
    """
    This endpoint is meant to essentially emulate the NWIS sites API, with the notable exception that there is no
    restriction on the number of hucs that can be requested, and that the data that are produced are in the web-friendly
    geoJSON format
    :return: a geojson feature collection
    """
    # request.args is an immutable multidict, so we need to turn it into something we can use.
    # It's a kinda weird thing with each value being a list, but requests knows what to do with it, so that's what we
    # are using.
    args = dict(request.args)
    args['format'] = 'rdb'  # we always want the format to be rdb
    #args['seriesCatalogOutput'] = True
    args['outputDataTypeCd'] = data_type
    args['parameterCd'] = parameter_code
    major_arguments_list = ['sites', 'stateCD', 'huc', 'bBox', 'countyCd']  # the NWIS sites service needs one of these
    # do this test so that we can take advantage of the NWIS site service validations
    if any(k in major_arguments_list for k in args.iterkeys()):
        nwis_head = head('http://waterservices.usgs.gov/nwis/site/', params=args)  # do a head request for validation
        if nwis_head.status_code == 200:  # hey, it's cool to do it with a single call, we can rock and roll
            return Response(generate_geojson_from_generator(args, parameter_code=parameter_code, period_of_record=True), content_type='application/json')
        # there is only allowed 1 HUC2 or 10 HUC8s in the query...and we are allowing as many as you want
        elif nwis_head.status_code == 400 and \
                ('Major HUC list size exceeded' in nwis_head.reason or 'Minor HUC list size exceeded' in nwis_head.reason):
            huc_string = args['huc'][0]  # get the request huc parameter string out of the multi-dict list
            small_huc_list = huc_string.split(',')  # turn comma-delimited huc parameter string into a list
            del args['huc']  # drop the huc parameter from the dict of arguments to send to the NWIS site service
            return Response(generate_geojson_from_generator(args, small_huc_list, parameter_code=parameter_code, period_of_record=True), content_type='application/json')
        else:
            resp = make_response(nwis_head.reason, nwis_head.status_code)
            resp.headers["X-Error-Reason"] = nwis_head.reason
            return resp
    else:
        # OK, we are gonna go for the whole country, hang onto your hats
        huc_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16',
                    '17', '18', '19', '20', '21']
        # Check to see if there is anything else is wrong with the query
        check_params = args
        check_params['huc'] = huc_list[0]
        nwis_head = head('http://waterservices.usgs.gov/nwis/site/', params=check_params)
        if nwis_head.status_code == 200:
            return Response(generate_geojson_from_generator(args, huc_list, parameter_code=parameter_code, period_of_record=True), content_type='application/json')
        else:
            resp = make_response(nwis_head.reason, nwis_head.status_code)
            resp.headers["X-Error-Reason"] = nwis_head.reason
            return resp


if __name__ == "__main__":
    app.run(debug=True)
