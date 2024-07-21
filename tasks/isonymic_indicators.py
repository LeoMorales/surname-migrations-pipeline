import pandas
from surnames_package import isonymic


def get_isonymy_departments_2015(product):
    """"""
    print("Computing get_isonymy_departments_2015...")
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet"
    )
    # df = pandas.read_parquet(surname_data_path)

    output_df = isonymic.get_isonymic_data_by_cells(
        df, slice_by_column="department_id", surname_column="surname"
    )
    output_df = output_df.rename(columns={"cell_id": "department_id"})
    output_df.to_parquet(str(product))


def get_isonymy_departments_2021(product):
    """"""
    print("Computing get_isonymy_departments_2021...")
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2021.parquet"
    )
    # df = pandas.read_parquet(surname_data_path)

    output_df = isonymic.get_isonymic_data_by_cells(
        df, slice_by_column="department_id", surname_column="surname"
    )
    output_df = output_df.rename(columns={"cell_id": "department_id"})
    output_df.to_parquet(str(product))
