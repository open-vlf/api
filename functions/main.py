import base64
import io
import os
import h5py
import boto3

import scipy.io as sio
import matplotlib.pyplot as plt

from mat73 import HDF5Decoder
from datetime import datetime
from matplotlib import rcParams
from astropy.io import fits

from firebase_functions import https_fn, options
from firebase_admin import initialize_app

from pymongo import MongoClient
from flask import jsonify

from plot_awesome import plot_awesome
from plot_savnet import plot_savnet

initialize_app()

# S3 client session
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
)
bucket: str = "craam-files-bucket"

# Plot setup
rcParams["figure.figsize"] = 7, 4
rcParams["figure.autolayout"] = True
rcParams["font.size"] = 12

client = MongoClient(os.getenv("MONGO_URI"))
database = client["main"]
collection = database["files"]


def mat_graph(object_buffer, path):
    try:
        # Handles v4 (Level 1.0), v6 and v7 to 7.2
        data = sio.loadmat(object_buffer)
    except ValueError:
        # Handles v7.3
        decoder = HDF5Decoder()

        try:
            with h5py.File(object_buffer) as hdf5:
                data = decoder.mat2dict(hdf5)["data"]

        except OSError:
            # File is corrupted or not in HDF5 format
            return https_fn.Response(
                status=400, response="File is corrupted or not in HDF5 format"
            )

    filename = path.split('/')[-1]
    fig, rc = plot_awesome(data, filename)

    if rc > 0:
        return https_fn.Response(status=400, response="Error while creating plot")

    # Export plot to a new buffer
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format="png")
    image_buffer.seek(0)

    # Convert image in bytes to base64 encoded
    base64_utf8_str = base64.b64encode(image_buffer.read()).decode("utf-8")

    return https_fn.Response(
        status=200, response=f"data:image/png;base64,{base64_utf8_str}"
    )


def fits_graph(object_buffer, path):
    fx = fits.open(object_buffer, memmap=True)

    filename = path.split('/')[-1]
    fig, rc = plot_savnet(fx, filename)

    if rc > 0:
        return https_fn.Response(status=400, response="Error while creating plot")

    # Export plot to a new buffer
    image_buffer = io.BytesIO()
    plt.savefig(image_buffer, format="png")
    image_buffer.seek(0)

    # Convert image in bytes to base64 encoded
    base64_utf8_str = base64.b64encode(image_buffer.read()).decode("utf-8")

    return https_fn.Response(
        status=200, response=f"data:image/png;base64,{base64_utf8_str}"
    )


@https_fn.on_request(cors=options.CorsOptions(cors_origins="*", cors_methods=["post"]))
def graph_generator(req: https_fn.Request) -> https_fn.Response:
    body_data = req.get_json(silent=True)

    if body_data is None or "path" not in body_data:
        return https_fn.Response(status=400, response="Path string missing")

    path = body_data["path"]

    # Download file data with buffer
    object_buffer = io.BytesIO()
    s3.download_fileobj(Bucket=bucket, Key=path, Fileobj=object_buffer)

    # Restart buffer pointer
    object_buffer.seek(0)

    if path.endswith(".fits"):
        return fits_graph(object_buffer, path)

    elif path.endswith(".mat"):
        return mat_graph(object_buffer, path)

    return https_fn.Response(status=404)


@https_fn.on_request(cors=options.CorsOptions(cors_origins="*", cors_methods=["get"]))
def get_years_stations(req: https_fn.Request) -> https_fn.Response:
    # My schema {"_id":{"$oid":"648a66f76b3f3f799f112d2e"},"fileName":"B1060406134536NPM_003A.mat","endpointType":"AWS S3",
    # "path":"2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","typeABCDF":"A","stationId":"B1","url":"https://craam-files-bucket.s3.sa-east-1.amazonaws.com/2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","dateTime":{"$date":{"$numberLong":"1144341936000"}},"timestamp":{"$date":{"$numberLong":"1686239656171"}},"CC":"03","transmitter":"NPM"}
    file_extension = req.args.get("fileEndsWith")
    match_cond = {}

    if file_extension:
        match_cond["path"] = {"$regex": file_extension.lower()}

    years = collection.aggregate(
        [
            {"$match": match_cond},
            {
                "$group": {
                    "_id": {"$year": "$dateTime"},
                    "stations": {"$addToSet": "$stationId"},
                }
            },
            {"$sort": {"_id": 1}},
        ]
    )

    years = list(years)

    response = [{"year": year["_id"], "stations": year["stations"]} for year in years]

    if len(response) == 0:
        return https_fn.Response(status=404, response="No data found")

    return jsonify(response)


@https_fn.on_request(cors=options.CorsOptions(cors_origins="*", cors_methods=["get"]))
def get_available_dates(req: https_fn.Request) -> https_fn.Response:
    # Query mongoDB for all available dates for the given station and year from the dateTime object
    # My schema {"_id":{"$oid":"648a66f76b3f3f799f112d2e"},"fileName":"B1060406134536NPM_003A.mat",
    # "endpointType":"AWS S3","path":"2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","typeABCDF":"A","stationId":"B1",
    # "url":"https://craam-files-bucket.s3.sa-east-1.amazonaws.com/2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","dateTime":{"$date":{"$numberLong":"1144341936000"}},"timestamp":{"$date":{"$numberLong":"1686239656171"}},"CC":"03","transmitter":"NPM"}
    # Args = stationId, example BA1, year, example 2006

    station = req.args.get("station")
    year = req.args.get("year")
    file_extension = req.args.get("fileEndsWith")

    if not station or not year:
        return https_fn.Response(status=400, response="Station or year missing")

    year = int(year)

    match_cond = {
        "stationId": station,
        "dateTime": {
            "$gte": datetime(year, 1, 1, 0, 0, 0, 0),
            "$lt": datetime(year + 1, 1, 1, 0, 0, 0, 0),
        },
    }

    if file_extension:
        match_cond["path"] = {"$regex": file_extension.lower()}

    available_dates = collection.aggregate(
        [
            {"$match": match_cond},
            {"$sort": {"dateTime": 1}},
        ]
    )

    # Values in the list must be unique
    narrowband = []
    broadband = []

    for date in list(available_dates):
        # Add 0 to the day and month if it is a single digit
        append_data = {
            "day": str(date["dateTime"].day).zfill(2),
            "month": str(date["dateTime"].month).zfill(2),
        }

        if "narrowband" in date["path"] and append_data not in narrowband:
            narrowband.append(append_data)

        elif "broadband" in date["path"] and append_data not in broadband:
            broadband.append(append_data)

    if len(broadband) == 0 and len(narrowband) == 0:
        return https_fn.Response(status=404, response="No data found")

    return jsonify(
        {
            "narrowband": narrowband,
            "broadband": broadband,
        }
    )


@https_fn.on_request(cors=options.CorsOptions(cors_origins="*", cors_methods=["get"]))
def get_available_files(req: https_fn.Request) -> https_fn.Response:
    # Query mongoDB for all available documents for the given station and year and type (narrowband or broadband)
    # My schema {"_id":{"$oid":"648a66f76b3f3f799f112d2e"},"fileName":"B1060406134536NPM_003A.mat",
    # "endpointType":"AWS S3","path":"2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","typeABCDF":"A","stationId":"B1",
    # "url":"https://craam-files-bucket.s3.sa-east-1.amazonaws.com/2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","dateTime":{"$date":{"$numberLong":"1144341936000"}},"timestamp":{"$date":{"$numberLong":"1686239656171"}},"CC":"03","transmitter":"NPM"}
    # Args = stationId, example BA1, year, example 2006
    station = req.args.get("station")
    type = req.args.get("type")
    file_extension = req.args.get("fileEndsWith")

    matchCond = {
        "dateTime": {
            "$gte": datetime(
                int(req.args.get("year")),
                int(req.args.get("month")),
                int(req.args.get("day")),
                0,
                0,
                0,
                0,
            ),
            "$lt": datetime(
                int(req.args.get("year")),
                int(req.args.get("month")),
                int(req.args.get("day")) + 1,
                0,
                0,
                0,
                0,
            ),
        },
    }

    if station:
        matchCond["stationId"] = station

    if type:
        matchCond["path"] = {"$regex": type.lower()}

    if file_extension:
        if file_extension == "fits":
            matchCond["path"] = {"$regex": "fits"}

    available_files = collection.aggregate(
        [
            {
                "$match": matchCond,
            },
            {"$sort": {"dateTime": 1}},
        ]
    )

    response = []

    for item in list(available_files):
        response.append(
            {
                "fileName": item.get("fileName"),
                "endpointType": item.get("endpointType"),
                "path": item.get("path"),
                "typeABCDF": item.get("typeABCDF"),
                "stationId": item.get("stationId"),
                "url": item.get("url"),
                "dateTime": item.get("dateTime").isoformat(),
                "CC": item.get("CC"),
                "transmitter": item.get("transmitter"),
            }
        )

    if len(response) == 0:
        return https_fn.Response(status=404, response="No data found")

    return jsonify(response)


@https_fn.on_request(cors=options.CorsOptions(cors_origins="*", cors_methods=["get"]))
def get_matrix(req: https_fn.Request) -> https_fn.Response:
    # My schema {"_id":{"$oid":"648a66f76b3f3f799f112d2e"},"fileName":"B1060406134536NPM_003A.mat","endpointType":"AWS S3",
    # "path":"2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","typeABCDF":"A","stationId":"B1","url":"https://craam-files-bucket.s3.sa-east-1.amazonaws.com/2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","dateTime":{"$date":{"$numberLong":"1144341936000"}},"timestamp":{"$date":{"$numberLong":"1686239656171"}},"CC":"03","transmitter":"NPM"}

    year = req.args.get("year")
    station = req.args.get("station")
    type = req.args.get("type")
    file_extension = req.args.get("fileEndsWith")

    if not year:
        year = "2006"

    year = int(year)

    matchCond = {
        "dateTime": {
            "$gte": datetime(year, 1, 1, 0, 0, 0, 0),
            "$lt": datetime(year + 1, 1, 1, 0, 0, 0, 0),
        },
    }

    if station:
        matchCond["stationId"] = station

    if type:
        matchCond["path"] = {"$regex": type.lower()}

    if file_extension:
        if file_extension == "fits":
            matchCond["path"] = {"$regex": "fits"}

    data = collection.aggregate(
        [
            {
                "$match": matchCond,
            },
            {
                "$group": {
                    "_id": {
                        "yearMonthDay": {
                            "$dateToString": {"format": "%Y-%m-%d", "date": "$dateTime"}
                        }
                    },
                    "stations": {"$addToSet": "$stationId"},
                    "count": {"$sum": 1},
                },
            },
            {"$sort": {"_id": 1}},
        ]
    )

    data = list(data)

    response = [
        {
            "date": item["_id"]["yearMonthDay"],
            "stations": item["stations"],
            "count": item["count"],
        }
        for item in data
    ]

    if len(response) == 0:
        return https_fn.Response(status=404, response="No data found")

    return jsonify(response)
