from flask_sqlalchemy import SQLAlchemy
from marshmallow import fields, Schema
from geoalchemy2.types import Geometry
from geoalchemy2.elements import WKTElement

db = SQLAlchemy()


class HeatmapModel(db.Model):
    __tablename__ = 'heatmap'
    __table_args__ = (db.PrimaryKeyConstraint('latitude', 'longitude'), {'schema': 'gc'})

    latitude = db.Column(db.Float, nullable=False)
    longitude = db.Column(db.Float, nullable=False)
    weight = db.Column(db.Integer, nullable=False)
    city = db.Column(db.String(), nullable=False)

    @staticmethod
    def get_all_by_city(value):
        return HeatmapModel.query.filter_by(city=value).all()

    def __repr(self):
        return f'Heatmap Point: {self.latitude}, {self.longitude}, {self.weight}, {self.city}'


class ListingModel(db.Model):
    __tablename__ = 'current_listings_sf'
    __table_args__ = ({'schema': 'gc'})

    id = db.Column(db.String(), primary_key=True)
    latitude = db.Column(db.Float, nullable=False)
    longitude = db.Column(db.Float, nullable=False)
    name = db.Column(db.String(), nullable=False)
    score = db.Column('final_score', db.Float, nullable=False)
    city = db.Column(db.String(), nullable=False)

    @staticmethod
    def get_all_by_city(value):
        return ListingModel.query.filter_by(city=value).all()

    def __repr(self):
        return f'Listing: {self.id}, {self.name}, {self.latitude}, {self.longitude}, {self.city}'


class SafetyInfoModel(db.Model):
    __tablename__ = 'safety_info_sf'
    __table_args__ = ({'schema': 'gc'})

    id = db.Column(db.String(), primary_key=True)
    date = db.Column('occur_date', db.Date, nullable=False)
    description = db.Column(nullable=True)
    latitude = db.Column(db.Float, nullable=False)
    longitude = db.Column(db.Float, nullable=False)
    # https://stackoverflow.com/questions/4069595/flask-with-geoalchemy-sample-code
    geom = db.Column(Geometry(geometry_type='POINT', srid=4326))

    @staticmethod
    def get_all_by_location(latitude, longitude):
        pt = WKTElement('POINT({0} {1})'.format(longitude, latitude), srid=4326)
        return SafetyInfoModel.query.order_by(SafetyInfoModel.geom.distance_box(pt)).limit(100).all()

    def __repr(self):
        return f'Listing: {self.id}, {self.name}, {self.latitude}, {self.longitude}, {self.city}'


class HeatmapSchema(Schema):
    latitude = fields.Float(dump_only=True, required=True)
    longitude = fields.Float(required=True)
    weight = fields.Int(dump_only=True)
    city = fields.Str(required=True)


class ListingSchema(Schema):
    id = fields.Str(required=True)
    latitude = fields.Float(required=True)
    longitude = fields.Float(required=True)
    score = fields.Float(required=True)
    name = fields.Str(required=True)
    city = fields.Str(required=True)


class SafetyInfoSchema(Schema):
    id = fields.Str(required=True, primary_key=True)
    date = fields.Date(required=True)
    description = fields.Str()
    city = fields.Str()
    latitude = fields.Float(required=True)
    longitude = fields.Float(required=True)
