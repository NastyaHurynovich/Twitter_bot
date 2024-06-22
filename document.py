from os import path

import pandas as pd


def read_txt(file):
    with open(file, "r") as f:
        users = f.read().splitlines()
    return users


def read_exel(file):
    excel_data = pd.read_excel(file)
    users = list(excel_data['Users'].tolist())
    return users


def read_file(file):
    if path.splitext(file)[-1] == ".txt":
        return read_txt(file)
    elif path.splitext(file)[-1] == ".xls" or ".xlsx":
        return read_exel(file)
