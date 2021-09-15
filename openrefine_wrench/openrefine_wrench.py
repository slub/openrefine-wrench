import logging
import logging.config
import pathlib
import uuid
import json
import click
from multiprocessing import Pool
from os import getpid
from openrefine_wrench.openrefine_api_calls import (
    create_or_project,
    apply_or_project,
    export_or_project_rows,
    delete_or_project)

logger = None

def _prep_logger(log_level, logfile):
    logging_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"}},
        "handlers": {
            "default": {
                "level": "INFO",
                "formatter": "standard",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr"},
            "logfile": {
                "level": "INFO",
                "formatter": "standard",
                "class": "logging.FileHandler",
                "filename": str(logfile),
                "mode": "w"}},
        "loggers": {
            "": {# root logger
                "handlers": ["default"],
                "level": log_level,
                "propagate": False}}}

    if logfile is not None:
        logging_config["loggers"][""]["handlers"][0] = "logfile"

    logging.config.dictConfig(logging_config)

    return logging.getLogger(__name__)

def _prep_options(
    source_format,
    record_path,
    columns_separator,
    encoding,
    custom_options):
    options = {
        "encoding": encoding,
        "storeEmptyStrings": True,
        "storeBlankCellsAsNulls": True,
        "storeBlankRows": True,
        "processQuotes": True,
        "columnWidths": None,
        "guessCellValueTypes": False,
        "headerLines": None,
        "ignoreLines": None,
        "includeFileSources": False,
        "limit": None,
        "linesPerRow": None,
        "projectName": None,
        "projectTags": None,
        "recordPath": None,
        "separator": None,
        "sheets": None,
        "skipDataLines": None,
        "trimStrings": False}

    if record_path is not None and source_format == "xml":
        options.update({"recordPath": record_path})
    elif columns_separator is not None and source_format == "csv":
        options.update({
            "separator": columns_separator,
            "ignore_lines": -1,
            "header_lines": 1,
            "skip_data_lines": 0,
            "limit": -1})

    if custom_options is not None:
        options.update(**json.loads(custom_options))

    return options

def _pool_handler(
    host,
    port,
    source_files,
    export_dir,
    options,
    or_project,
    source_format,
    max_workers):

    params = [(
        host,
        port,
        str(file),
        export_dir,
        options,
        source_format,
        or_project) for file in source_files]

    with(Pool(max_workers)) as p:
        logger.info(f"we spawn over {max_workers} workers")
        p.starmap(_run_or_processing, params)

def _run_or_processing(
    host,
    port,
    project_file,
    export_dir,
    options,
    source_format,
    or_project):

    pid = getpid()

    logger.info(f"[pid {pid}] start or processing for file {project_file}")

    project_name = f"{pathlib.Path(project_file).stem}_{str(uuid.uuid4())}"

    project_id = create_or_project(
        host=host,
        port=port,
        pid=pid,
        project_file=project_file,
        project_name=project_name,
        source_format=source_format,
        options=options)

    apply_or_project(
        host=host,
        port=port,
        pid=pid,
        project_id=project_id,
        or_project=or_project)

    export_or_project_rows(
        host=host,
        port=port,
        pid=pid,
        project_id=project_id,
        export_format="csv",
        project_file=project_file,
        export_dir=export_dir)

    delete_or_project(
        host=host,
        port=port,
        pid=pid,
        project_id=project_id)

    logger.info(f"[pid {pid}] done with or processing for file {project_file}")

@click.command()
@click.option(
    "--host",
    help="openrefine host",
    required=True)
@click.option(
    "--port",
    help="openrefine port (default to 3333)",
    default="3333",
    type=str,
    required=True)
@click.option(
    "--source-dir",
    help="openrefine source data dir",
    required=True)
@click.option(
    "--export-dir",
    help="openrefine export data dir",
    required=True)
@click.option(
    "--source-format",
    help="openrefine source data format",
    type=click.Choice(["xml", "csv"], case_sensitive=False),
    required=True)
@click.option(
    "--encoding",
    help="openrefine source data encoding (default to UTF-8)",
    default="UTF-8",
    type=str,
    required=True)
@click.option(
    "--record-path",
    help="record path (only applicable in conjunction with xml source format)",
    type=str,
    multiple=True)
@click.option(
    "--columns-separator",
    help="columns separator (only applicable in conjunction with csv source format)",
    type=str,
    default=",")
@click.option(
    "--mappings-file",
    help="openrefine mappings file",
    required=True)
@click.option(
    "--max-workers",
    help="number of parallel processed openrefine projects",
    default=1,
    type=int,
    required=True)
@click.option(
    "--log-level",
    help="log level (default INFO)",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "OFF"]), default="INFO")
@click.option(
    "--custom-options",
    help="custom options (overrides everything, only in case you know what you're doing)",
    type=str)
@click.option(
    "--logfile",
    help="openrefine-wrench related logfile",
    default=None,
    type=str)
def openrefine_wrench(
    host,
    port,
    source_dir,
    export_dir,
    source_format,
    encoding,
    record_path,
    columns_separator,
    mappings_file,
    max_workers,
    log_level,
    custom_options,
    logfile):
    """Handle multiple input files in separte openrefine projects."""

    global logger
    logger = _prep_logger(log_level, logfile)

    options = _prep_options(
        source_format,
        record_path,
        columns_separator,
        encoding,
        custom_options)

    source_files = pathlib.Path(source_dir).glob(f"*.{source_format}")
    or_project = None

    with(open(file=mappings_file, mode="r", encoding="UTF-8")) as fi:
        or_project = json.loads(fi.read())

    _pool_handler(
        host=host,
        port=port,
        source_files=source_files,
        export_dir=export_dir,
        options=options,
        or_project=or_project,
        source_format=source_format,
        max_workers=max_workers)

@click.command()
@click.option(
    "--host",
    help="openrefine host",
    required=True)
@click.option(
    "--port",
    help="openrefine port (default to 3333)",
    default="3333",
    type=str,
    required=True)
@click.option(
    "--source-file",
    help="openrefine source file",
    required=True)
@click.option(
    "--source-format",
    help="openrefine source data format",
    type=click.Choice(["xml", "csv"], case_sensitive=False),
    required=True)
@click.option(
    "--project-name",
    help="openrefine project name",
    required=True)
@click.option(
    "--encoding",
    help="openrefine source data encoding (default to UTF-8)",
    default="UTF-8",
    type=str,
    required=True)
@click.option(
    "--record-path",
    help="record path (only applicable in conjunction with xml source format)",
    type=str,
    multiple=True)
@click.option(
    "--columns-separator",
    help="columns separator (only applicable in conjunction with csv source format)",
    type=str,
    default=",")
@click.option(
    "--log-level",
    help="log level (default INFO)",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "OFF"]), default="INFO")
@click.option(
    "--custom-options",
    help="custom options (overrides everything, only in case you know what you're doing)",
    type=str)
@click.option(
    "--logfile",
    help="openrefine-wrench-create related logfile",
    default=None,
    type=str)
def openrefine_wrench_create(
    host,
    port,
    source_file,
    source_format,
    project_name,
    encoding,
    record_path,
    columns_separator,
    log_level,
    custom_options,
    logfile):
    """Create single openrefine project."""

    global logger
    logger = _prep_logger(log_level, logfile)

    pid = getpid()

    options = _prep_options(
        source_format,
        record_path,
        columns_separator,
        encoding,
        custom_options)

    create_or_project(
        host=host,
        port=port,
        pid=pid,
        project_file=source_file,
        project_name=project_name,
        source_format=source_format,
        options=options)

@click.command()
@click.option(
    "--host",
    help="openrefine host",
    required=True)
@click.option(
    "--port",
    help="openrefine port (default to 3333)",
    default="3333",
    type=str,
    required=True)
@click.option(
    "--project-id",
    help="openrefine project id",
    required=True)
@click.option(
    "--mappings-file",
    help="openrefine mappings file",
    required=True)
@click.option(
    "--log-level",
    help="log level (default INFO)",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "OFF"]), default="INFO")
@click.option(
    "--logfile",
    help="openrefine-wrench-apply related logfile",
    default=None,
    type=str)
def openrefine_wrench_apply(
    host,
    port,
    project_id,
    mappings_file,
    log_level,
    logfile):
    """Apply rules to single openrefine project."""

    global logger
    logger = _prep_logger(log_level, logfile)

    pid = getpid()

    or_project = None
    with(open(file=mappings_file, mode="r", encoding="UTF-8")) as fi:
        or_project = json.loads(fi.read())

    apply_or_project(
        host=host,
        port=port,
        pid=pid,
        project_id=project_id,
        or_project=or_project)

@click.command()
@click.option(
    "--host",
    help="openrefine host",
    required=True)
@click.option(
    "--port",
    help="openrefine port (default to 3333)",
    default="3333",
    type=str,
    required=True)
@click.option(
    "--export-file",
    help="openrefine export file",
    required=True)
@click.option(
    "--project-id",
    help="openrefine project id",
    required=True)
@click.option(
    "--log-level",
    help="log level (default INFO)",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "OFF"]), default="INFO")
@click.option(
    "--logfile",
    help="openrefine-wrench-export related logfile",
    default=None,
    type=str)
def openrefine_wrench_export(
    host,
    port,
    project_id,
    export_file,
    log_level,
    logfile):
    """Export single modified openrefine project."""

    global logger
    logger = _prep_logger(log_level, logfile)

    pid = getpid()

    export_dir = str(pathlib.Path(export_file).parent)

    export_or_project_rows(
        host=host,
        port=port,
        pid=pid,
        project_id=project_id,
        export_format="csv",
        project_file=export_file,
        export_dir=export_dir)

@click.command()
@click.option(
    "--host",
    help="openrefine host",
    required=True)
@click.option(
    "--port",
    help="openrefine port (default to 3333)",
    default="3333",
    type=str,
    required=True)
@click.option(
    "--project-id",
    help="openrefine project id",
    required=True)
@click.option(
    "--log-level",
    help="log level (default INFO)",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "OFF"]), default="INFO")
@click.option(
    "--logfile",
    help="openrefine-wrench-delete related logfile",
    default=None,
    type=str)
def openrefine_wrench_delete(
    host,
    port,
    project_id,
    log_level,
    logfile):
    """Delete single openrefine project."""

    global logger
    logger = _prep_logger(log_level, logfile)

    pid = getpid()

    delete_or_project(
        host=host,
        port=port,
        pid=pid,
        project_id=project_id)
