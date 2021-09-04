from context import openrefine_wrench

wanted = {
    "columnWidths": None,
    "encoding": "UTF-8",
    "guessCellValueTypes": False,
    "headerLines": None,
    "header_lines": 1,
    "ignoreLines": None,
    "ignore_lines": -1,
    "includeFileSources": False,
    "limit": -1,
    "linesPerRow": None,
    "processQuotes": True,
    "projectName": None,
    "projectTags": None,
    "recordPath": None,
    "separator": "#",
    "sheets": None,
    "skipDataLines": None,
    "skip_data_lines": 0,
    "storeBlankCellsAsNulls": True,
    "storeBlankRows": True,
    "storeEmptyStrings": True,
    "trimStrings": False}

def test_prep_options():
    options = openrefine_wrench._prep_options(
        source_format="csv",
        record_path=None,
        columns_separator=",",
        encoding=None,
        custom_options='{"encoding": "UTF-8", "separator": "#"}')

    assert options == wanted
