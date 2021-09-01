from context import openrefine_wrench

def test_prep_options():
    options = openrefine_wrench._prep_options(
        source_format="csv",
        record_path=None,
        columns_separator=",",
        encoding=None,
        custom_options='{"encoding": "UTF-8", "separator": "#"}')

    assert options == {
        "encoding": "UTF-8",
        "separator": "#",
        "ignore_lines": -1,
        "header_lines": 1,
        "skip_data_lines": 0,
        "limit": -1,
        "store_blank_rows": True,
        "guess_cell_value_types": False,
        "process_quotes": True,
        "store_blank_cells_as_nulls": True,
        "include_file_sources": False}
