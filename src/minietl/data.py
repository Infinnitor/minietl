import mimetypes
import openpyxl
import csv
import json

from os import PathLike


PathType = str | PathLike


def get_csv_data(path: PathType):
    with open(path, "r") as fp:
        reader = csv.reader(fp)
        return list(reader)


def get_json_data(path: PathType):
    with open(path, "r") as fp:
        return json.load(fp)


def get_excel_data(path: PathType):
    wb = openpyxl.load_workbook(path, data_only=True)
    sheet = wb.active

    rows = []
    for row in sheet.iter_rows():
        rows.append([row[i].value for i in range(len(row))])

    return rows


def get_data_auto(path: PathType):
    ft, _ = mimetypes.guess_type(path)

    match ft:
        case "application/json":
            return get_json_data(path)
        case "text/csv":
            return get_csv_data(path)
        case "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
            return get_excel_data(path)
