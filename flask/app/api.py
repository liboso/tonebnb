from flask import request, json, Response, Blueprint, g
from models import *

heatmap_api = Blueprint('heatmap', __name__)
listing_api = Blueprint('listing', __name__)
safety_info_api = Blueprint('safetyinfo', __name__)


@heatmap_api.route('/<city>', methods=['GET'])
def get_heat_points(city):
    locations = HeatmapModel.get_all_by_city(city)
    ser_locations = HeatmapSchema().dump(locations, many=True).data
    return custom_response(ser_locations, 200)


@listing_api.route('/<city>', methods=['GET'])
def get_listings(city):
    listings = ListingModel.get_all_by_city(city)
    ser_listings = ListingSchema().dump(listings, many=True).data
    return custom_response(ser_listings, 200)


@safety_info_api.route('/', methods=['GET'])
def get_nearby_safety_infos():
    args = request.args
    latitude = float(args['latitude'])
    longitude = float(args['longitude'])
    safety_infos = SafetyInfoModel.get_all_by_location(latitude, longitude)
    ser_safety_infos = SafetyInfoSchema().dump(safety_infos, many=True).data
    return custom_response(ser_safety_infos, 200)


def custom_response(res, status_code):
    return Response(
        mimetype="application/json",
        response=json.dumps(res),
        status=status_code
    )