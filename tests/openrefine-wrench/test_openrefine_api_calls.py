import csv
import json
import pytest
import pathlib
from os import getpid
from tempfile import TemporaryDirectory
from requests.exceptions import RequestException

from context import openrefine_api_calls, openrefine_wrench

csv_sample_data = [
    {"first_name": "Baked", "last_name": "Beans"},
    {"first_name": "Lovely", "last_name": "Spam"},
    {"first_name": "Wonderful", "last_name": "Spam"}]

csv_wanted_data = [
    {"first_name": "Baked", "last_name": "BEANS"},
    {"first_name": "Lovely", "last_name": "SPAM"},
    {"first_name": "Wonderful", "last_name": "SPAM"}]

csv_or_transformation = """[
  {
    "op": "core/text-transform",
    "engineConfig": {
      "facets": [],
      "mode": "row-based"
    },
    "columnName": "last_name",
    "expression": "value.toUppercase()",
    "onError": "keep-original",
    "repeat": false,
    "repeatCount": 10,
    "description": "Text transform on cells in column last_name using expression value.toUppercase()"
  }
]"""

def _is_responsive(docker_ip, port):
    resp_csrf_token = None
    try:
        resp_csrf_token = openrefine_api_calls._get_csrf_token(
            host=docker_ip,
            port=port,
            pid=getpid())

        if resp_csrf_token is not None:
            return True
    except RequestException as exc:
        return False

@pytest.fixture(scope="session")
def _docker_handler(docker_ip, docker_services):
    port = docker_services.port_for("test_openrefine", 3333)

    docker_services.wait_until_responsive(
        timeout=60.0, pause=5.0, check=lambda: _is_responsive(docker_ip, port)
    )

    return True

def _create_csv_test_data(csv_test_data_dir):
    csv_test_file = f"{csv_test_data_dir}/test.csv"
    with open(
        csv_test_file,
        "w",
        newline="",
        encoding="UTF-8") as csvfile:
        fieldnames = ['first_name', 'last_name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for set in csv_sample_data:
            writer.writerow(set)

    return csv_test_file

def _get_export_data(export_file):
    csv_export_data = None

    with open(export_file, mode="r", encoding="UTF-8") as fi:
        reader = csv.DictReader(fi)
        csv_export_data = [row for row in reader]

    return csv_export_data

def test_csv_or_project(_docker_handler):
    assert _docker_handler == True

    with TemporaryDirectory() as csv_test_data_dir:
        csv_test_file = _create_csv_test_data(csv_test_data_dir)

        project_id = openrefine_api_calls.create_or_project(
            host="localhost",
            port="3333",
            pid=getpid(),
            project_file=csv_test_file,
            project_name="or_csv_test_project",
            source_format="csv",
            options=openrefine_wrench._prep_options(
                source_format="csv",
                record_path=None,
                columns_separator=",",
                encoding=None,
                custom_options=None))

        apply_resp = openrefine_api_calls.apply_or_project(
            host="localhost",
            port="3333",
            pid=getpid(),
            project_id=project_id,
            or_project=json.loads(csv_or_transformation))

        assert apply_resp == True

        export_file = openrefine_api_calls.export_or_project_rows(
            host="localhost",
            port="3333",
            pid=getpid(),
            project_id=project_id,
            export_format="csv",
            project_file=str(pathlib.Path(csv_test_file).with_name("test_export.csv")),
            export_dir=csv_test_data_dir)

        assert export_file == f"{csv_test_data_dir}/test_export.csv"
        assert _get_export_data(export_file) == csv_wanted_data

        delete_resp = openrefine_api_calls.delete_or_project(
            host="localhost",
            port="3333",
            pid=getpid(),
            project_id=project_id)

        assert delete_resp == "ok"
