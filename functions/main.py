import base64
import io
import os
import boto3
import re

from datetime import datetime, timezone

from firebase_functions import https_fn, options
from firebase_admin import initialize_app, auth, app_check
from google.auth import default as google_auth_default
from google.cloud import firestore
from flask import jsonify


initialize_app()
credentials, project = google_auth_default()
database_id = os.getenv("FIRESTORE_DATABASE", "open-vlf")
project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or project
db = firestore.Client(
    project=project_id,
    credentials=credentials,
    database=database_id,
)

FILES_BY_DAY_COLLECTION = "files_by_day"
YEARS_STATIONS_COLLECTION = "years_stations"
AVAILABLE_DATES_COLLECTION = "available_dates"
MATRIX_COLLECTION = "matrix"
ALLOWED_EXTENSIONS = {"mat", "fits"}
ALLOWED_TYPES = {"narrowband", "broadband"}
STATION_RE = re.compile(r"^[A-Za-z0-9]{2,4}$")
MIN_YEAR = 2006
MAX_YEAR = 2026
DEFAULT_CACHE_SECONDS = 600

# S3 client session
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("AWS_SECRET_KEY"),
)
bucket: str = "craam-files-bucket"

def init_plot():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib import rcParams

    rcParams["figure.figsize"] = 7, 4
    rcParams["figure.autolayout"] = True
    rcParams["font.size"] = 12
    return plt

def serialize_datetime(value):
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.isoformat()
    return value


def json_response(payload, cache_seconds=DEFAULT_CACHE_SECONDS):
    response = jsonify(payload)
    if cache_seconds:
        response.headers["Cache-Control"] = f"public, max-age={cache_seconds}"
    return response


def normalize_extension(value):
    if not value:
        return None
    ext = value.lower()
    if ext not in ALLOWED_EXTENSIONS:
        return None
    return ext


def normalize_type(value):
    if not value:
        return None
    data_type = value.lower()
    if data_type not in ALLOWED_TYPES:
        return None
    return data_type


def parse_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def valid_station(value):
    return bool(value and STATION_RE.match(value))


def verify_request(req: https_fn.Request) -> bool:
    if req.method == "OPTIONS":
        return True
    if os.getenv("AUTH_DISABLED", "").lower() == "true":
        return True

    auth_header = req.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return False

    id_token = auth_header.split("Bearer ", 1)[1].strip()
    if not id_token:
        return False

    app_check_token = req.headers.get("X-Firebase-AppCheck")
    if not app_check_token:
        return False

    try:
        auth.verify_id_token(id_token)
        app_check.verify_token(app_check_token)
    except Exception:
        return False

    return True


def mat_graph(object_buffer, path):
    import h5py
    import scipy.io as sio
    from mat73 import HDF5Decoder
    from plot_awesome import plot_awesome

    plt = init_plot()
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
    from astropy.io import fits
    from plot_savnet import plot_savnet

    plt = init_plot()
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
    if not verify_request(req):
        return https_fn.Response(status=401, response="Unauthorized")
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
    if not verify_request(req):
        return https_fn.Response(status=401, response="Unauthorized")
    # My schema {"_id":{"$oid":"648a66f76b3f3f799f112d2e"},"fileName":"B1060406134536NPM_003A.mat","endpointType":"AWS S3",
    # "path":"2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","typeABCDF":"A","stationId":"B1","url":"https://craam-files-bucket.s3.sa-east-1.amazonaws.com/2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","dateTime":{"$date":{"$numberLong":"1144341936000"}},"timestamp":{"$date":{"$numberLong":"1686239656171"}},"CC":"03","transmitter":"NPM"}
    raw_extension = req.args.get("fileEndsWith")
    file_extension = normalize_extension(raw_extension)
    if raw_extension and not file_extension:
        return https_fn.Response(status=400, response="Invalid parameters")
    collection_ref = db.collection(YEARS_STATIONS_COLLECTION)

    if file_extension:
        file_extension = file_extension.lower()
        docs = collection_ref.where("extension", "==", file_extension).stream()
        response = [
            {
                "year": doc.to_dict().get("year"),
                "stations": doc.to_dict().get("stations", []),
            }
            for doc in docs
        ]
        response.sort(key=lambda item: item["year"])
    else:
        years_map = {}
        for doc in collection_ref.stream():
            data = doc.to_dict()
            year = data.get("year")
            stations = data.get("stations", [])
            if year not in years_map:
                years_map[year] = set()
            years_map[year].update(stations)
        response = [
            {"year": year, "stations": sorted(stations)}
            for year, stations in sorted(years_map.items())
        ]

    if len(response) == 0:
        return https_fn.Response(status=404, response="No data found")

    return json_response(response)


@https_fn.on_request(cors=options.CorsOptions(cors_origins="*", cors_methods=["get"]))
def get_available_dates(req: https_fn.Request) -> https_fn.Response:
    if not verify_request(req):
        return https_fn.Response(status=401, response="Unauthorized")
    # Query mongoDB for all available dates for the given station and year from the dateTime object
    # My schema {"_id":{"$oid":"648a66f76b3f3f799f112d2e"},"fileName":"B1060406134536NPM_003A.mat",
    # "endpointType":"AWS S3","path":"2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","typeABCDF":"A","stationId":"B1",
    # "url":"https://craam-files-bucket.s3.sa-east-1.amazonaws.com/2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","dateTime":{"$date":{"$numberLong":"1144341936000"}},"timestamp":{"$date":{"$numberLong":"1686239656171"}},"CC":"03","transmitter":"NPM"}
    # Args = stationId, example BA1, year, example 2006

    station = req.args.get("station")
    year = req.args.get("year")
    raw_extension = req.args.get("fileEndsWith")
    file_extension = normalize_extension(raw_extension)

    if not station or not year:
        return https_fn.Response(status=400, response="Station or year missing")

    year = parse_int(year)
    if (
        year is None
        or year < MIN_YEAR
        or year > MAX_YEAR
        or not valid_station(station)
        or (raw_extension and not file_extension)
    ):
        return https_fn.Response(status=400, response="Invalid parameters")
    collection_ref = db.collection(AVAILABLE_DATES_COLLECTION)
    query = collection_ref.where("stationId", "==", station).where("year", "==", year)

    if file_extension:
        query = query.where("extension", "==", file_extension)

    docs = list(query.stream())

    if not docs:
        return https_fn.Response(status=404, response="No data found")

    narrowband_set = set()
    broadband_set = set()

    for doc in docs:
        data = doc.to_dict()
        for item in data.get("narrowband", []):
            narrowband_set.add((item.get("month"), item.get("day")))
        for item in data.get("broadband", []):
            broadband_set.add((item.get("month"), item.get("day")))

    narrowband = [
        {"day": day, "month": month}
        for month, day in sorted(narrowband_set)
    ]
    broadband = [
        {"day": day, "month": month}
        for month, day in sorted(broadband_set)
    ]

    if len(broadband) == 0 and len(narrowband) == 0:
        return https_fn.Response(status=404, response="No data found")

    return json_response(
        {
            "narrowband": narrowband,
            "broadband": broadband,
        }
    )


@https_fn.on_request(cors=options.CorsOptions(cors_origins="*", cors_methods=["get"]))
def get_available_files(req: https_fn.Request) -> https_fn.Response:
    if not verify_request(req):
        return https_fn.Response(status=401, response="Unauthorized")
    # Query mongoDB for all available documents for the given station and year and type (narrowband or broadband)
    # My schema {"_id":{"$oid":"648a66f76b3f3f799f112d2e"},"fileName":"B1060406134536NPM_003A.mat",
    # "endpointType":"AWS S3","path":"2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","typeABCDF":"A","stationId":"B1",
    # "url":"https://craam-files-bucket.s3.sa-east-1.amazonaws.com/2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","dateTime":{"$date":{"$numberLong":"1144341936000"}},"timestamp":{"$date":{"$numberLong":"1686239656171"}},"CC":"03","transmitter":"NPM"}
    # Args = stationId, example BA1, year, example 2006
    station = req.args.get("station")
    raw_type = req.args.get("type")
    file_type = normalize_type(raw_type)
    raw_extension = req.args.get("fileEndsWith")
    file_extension = normalize_extension(raw_extension)

    year = req.args.get("year")
    month = req.args.get("month")
    day = req.args.get("day")

    if not year or not month or not day:
        return https_fn.Response(status=400, response="Date missing")

    year = parse_int(year)
    month = parse_int(month)
    day = parse_int(day)

    if (
        year is None
        or month is None
        or day is None
        or year < MIN_YEAR
        or year > MAX_YEAR
        or not (1 <= month <= 12)
        or not (1 <= day <= 31)
        or (station and not valid_station(station))
        or (raw_type and not file_type)
        or (raw_extension and not file_extension)
    ):
        return https_fn.Response(status=400, response="Invalid parameters")

    extension = file_extension if file_extension else "mat"
    if extension != "fits":
        extension = "mat"

    date_str = f"{year:04d}-{month:02d}-{day:02d}"
    collection_ref = db.collection(FILES_BY_DAY_COLLECTION)

    files = []

    if station and file_type:
        doc_id = f"{date_str}_{station}_{file_type}_{extension}"
        doc = collection_ref.document(doc_id).get()
        if doc.exists:
            files = doc.to_dict().get("files", [])
    else:
        query = collection_ref.where("date", "==", date_str).where(
            "extension", "==", extension
        )
        if station:
            query = query.where("stationId", "==", station)
        if file_type:
            query = query.where("type", "==", file_type)
        docs = list(query.stream())
        for doc in docs:
            files.extend(doc.to_dict().get("files", []))

    response = []

    for item in sorted(files, key=lambda entry: entry.get("dateTime")):
        response.append(
            {
                "fileName": item.get("fileName"),
                "endpointType": item.get("endpointType"),
                "path": item.get("path"),
                "typeABCDF": item.get("typeABCDF"),
                "stationId": item.get("stationId"),
                "url": item.get("url"),
                "dateTime": serialize_datetime(item.get("dateTime")),
                "CC": item.get("CC"),
                "transmitter": item.get("transmitter"),
            }
        )

    if len(response) == 0:
        return https_fn.Response(status=404, response="No data found")

    return json_response(response)


@https_fn.on_request(cors=options.CorsOptions(cors_origins="*", cors_methods=["get"]))
def get_matrix(req: https_fn.Request) -> https_fn.Response:
    if not verify_request(req):
        return https_fn.Response(status=401, response="Unauthorized")
    # My schema {"_id":{"$oid":"648a66f76b3f3f799f112d2e"},"fileName":"B1060406134536NPM_003A.mat","endpointType":"AWS S3",
    # "path":"2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","typeABCDF":"A","stationId":"B1","url":"https://craam-files-bucket.s3.sa-east-1.amazonaws.com/2006/04/06/narrowband/B1/B1060406134536NPM_003A.mat","dateTime":{"$date":{"$numberLong":"1144341936000"}},"timestamp":{"$date":{"$numberLong":"1686239656171"}},"CC":"03","transmitter":"NPM"}

    year = req.args.get("year")
    station = req.args.get("station")
    raw_type = req.args.get("type")
    file_type = normalize_type(raw_type)
    raw_extension = req.args.get("fileEndsWith")
    file_extension = normalize_extension(raw_extension)

    if not year:
        year = MIN_YEAR
    else:
        year = parse_int(year)

    if (
        year is None
        or year < MIN_YEAR
        or year > MAX_YEAR
        or (station and not valid_station(station))
        or (raw_type and not file_type)
        or (raw_extension and not file_extension)
    ):
        return https_fn.Response(status=400, response="Invalid parameters")

    extension = file_extension if file_extension else "mat"
    if extension != "fits":
        extension = "mat"

    if not station and not file_type:
        doc_id = f"{extension}_{year}"
        doc = db.collection(MATRIX_COLLECTION).document(doc_id).get()
        if not doc.exists:
            return https_fn.Response(status=404, response="No data found")
        data = doc.to_dict()
        items = data.get("items", [])
        response = sorted(items, key=lambda item: item.get("date"))
    else:
        query = (
            db.collection(FILES_BY_DAY_COLLECTION)
            .where("year", "==", year)
            .where("extension", "==", extension)
        )

        if station:
            query = query.where("stationId", "==", station)
        if file_type:
            query = query.where("type", "==", file_type)

        docs = list(query.stream())
        matrix_map = {}

        for doc in docs:
            data = doc.to_dict()
            date = data.get("date")
            station_id = data.get("stationId")
            count = data.get("fileCount", len(data.get("files", [])))

            if date not in matrix_map:
                matrix_map[date] = {"stations": set(), "count": 0}

            matrix_map[date]["count"] += count
            matrix_map[date]["stations"].add(station_id)

        response = [
            {
                "date": date,
                "stations": sorted(values["stations"]),
                "count": values["count"],
            }
            for date, values in sorted(matrix_map.items())
        ]

    if len(response) == 0:
        return https_fn.Response(status=404, response="No data found")

    return json_response(response)
