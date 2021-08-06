import pathlib
import logging
import requests
import json
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

def _get_csrf_token(host, port, pid):
    resp_csrf_token = None
    try:
        resp_csrf_token = requests.get(
            f"http://{host}:{port}/command/core/get-csrf-token")
    except requests.exceptions.RequestException as exc:
        logger.error(f"[pid {pid}] unable to get csrf-token, error was:\n{exc}")
        raise

    return resp_csrf_token.json()["token"]

def create_or_project(
    host,
    port,
    pid,
    project_file,
    project_name,
    source_format,
    options):
    """Create openrefine project.

    Args:
        host:           base url of the used openrefine host
        port:           openrefine port
        pip:            process id
        project_file:   source file
        project_name:   name of the openrefine project
        source_format:  format of the source data (limited to csv or xml)
        options:        e.g. encoding and recordPath
                        ({"encoding": "UTF-8", "recordPath": ["Records", "record"]})

    Returns:
        project_id:     id of the created openrefine project
    """

    csrf_token = _get_csrf_token(host, port, pid)

    payload = {
        "project-name": project_name,}

    if source_format is "xml":
        payload.update({"format": "text/xml"})

    if options is not None:
        payload.update({"options": f"{json.dumps(options)}"})

    files = {"project-file": (project_file, open(project_file, "rb"))}

    resp_project_create = None

    try:
        resp_project_create = requests.post(
            f"http://{host}:{port}/command/core/create-project-from-upload?csrf_token={csrf_token}",
            data=payload, files=files)
    except requests.exceptions.RequestException as exc:
        logger.error(f"[pid {pid}] unable to create project for file {project_file}, error was:\n{exc}")
        raise

    url_frag = urlparse(resp_project_create.request.url)

    project_id = url_frag.query.split("=")[1]

    logger.info(f"[pid {pid}] created project with id {project_id} for file {project_file} ")

    return project_id

def apply_or_project(
    host,
    port,
    pid,
    project_id,
    or_project):
    """Apply rules to openrefine project.

    Args:
        host:           base url of the used openrefine host
        port:           openrefine port
        pip:            process id
        project_id:     id of the created openrefine project
        or_project:     json string containing all project related rules

    Returns:
        response code:  openrefine api response code, "ok" if application succeeded
    """

    csrf_token = _get_csrf_token(host, port, pid)

    payload = {
        "project": project_id,
        "operations": json.dumps(or_project),}

    resp_project_apply = None

    try:
        resp_project_apply = requests.post(
            f"http://{host}:{port}/command/core/apply-operations?csrf_token={csrf_token}",
            data=payload)
    except requests.exceptions.RequestException as exc:
        logger.error(f"[pid {pid}] unable to apply or project id {project_id}, error was:\n{exc}")
        raise

    logger.info(f"[pid {pid}] applied project id {project_id}")

    return resp_project_apply.json()["code"]

def export_or_project_rows(
    host,
    port,
    pid,
    project_id,
    export_format,
    project_file,
    export_dir):
    """Export all project related rows from openrefine.

    The export file format is limited to csv only!

    Args:
        host:           base url of the used openrefine host
        port:           openrefine port
        pip:            process id
        project_id:     id of the created openrefine project
        export_format:  limited to csv
        project_file:   project source file
        export_dir:     path of the directory to export to
    """

    export_file = f"{export_dir}/{str(pathlib.Path(project_file).with_suffix('.csv').name)}"

    csrf_token = _get_csrf_token(host, port, pid)

    payload = {
        "project": project_id,
        "format": export_format}

    resp_project_rows_export = None

    try:
        resp_project_rows_export = requests.post(
            f"http://{host}:{port}/command/core/export-rows?csrf_token={csrf_token}",
            data=payload)
    except requests.exceptions.RequestException as exc:
        logger.error(
            f"[pid {pid}] unable to export or project id {project_id}, "
            f"to export file {export_file}, error was:\n{exc}")
        raise

    if resp_project_rows_export is not None:
        with(open(file=export_file, mode="w", encoding="UTF-8")) as fo:
            fo.write(resp_project_rows_export.text)

        logger.info(
            f"[pid {pid}] exported or project with id {project_id} "
            f"to export file {export_file}")

def delete_or_project(
    host,
    port,
    pid,
    project_id):
    """Delete openrefine project.

    Args:
        host:           base url of the used openrefine host
        port:           openrefine port
        pip:            process id
        project_id:     id of the created openrefine project

    Returns:
        response code:  openrefine api response code, "ok" if deletion succeeded
    """

    csrf_token = _get_csrf_token(host, port, pid)

    payload = {"project": project_id}

    resp_project_delete = None

    try:
        resp_project_delete = requests.post(
            f"http://{host}:{port}/command/core/delete-project?csrf_token={csrf_token}",
            data=payload)
    except requests.exceptions.RequestException as exc:
        logger.error(
            f"[pid {pid}] unable to delete or project id {project_id}, error was:\n{exc}")
        raise

    logger.info(f"[pid {pid}] deleted or project with id {project_id}")

    return resp_project_delete.json()["code"]
