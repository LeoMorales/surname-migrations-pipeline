import pandas


def get_isonymy_and_migration_indicators_2001(upstream, product):
    departamental_df = pandas.merge(
        pandas.read_parquet(str(upstream["get-departmental-karlin-mcgregor-2001"])),
        pandas.read_parquet(str(upstream["get-departmental-wright-m-2001"])),
        on="department_id",
    )

    col_order = [
        "department_id",
        "n",
        "ins",
        "fst",
        "fishers_alpha",
        "a",
        "b",
        "v",
        "m",
        "population_2001",
    ]

    departamental_df[col_order].to_parquet(str(product))


def get_isonymy_and_migration_indicators_2015(upstream, product):
    DIVISION_KEY = "department_id"

    departamental_df = pandas.merge(
        pandas.merge(
            pandas.read_parquet(str(upstream["get-departmental-karlin-mcgregor-2015"])),
            pandas.read_parquet(str(upstream["get-departmental-isonymy-2015"])).rename(
                columns={"division": DIVISION_KEY}
            ),
            on="department_id",
        ),
        pandas.read_parquet(str(upstream["get-departmental-wright-m-2015"])),
        on="department_id",
    )

    departamental_df.columns = [column.lower() for column in departamental_df.columns]
    col_order = [
        "department_id",
        "n",
        "ins",
        "fst",
        "fishers_alpha",
        "a",
        "b",
        "v",
        "m",
        "population_2015",
    ]

    departamental_df = departamental_df[col_order]
    departamental_df.to_parquet(str(product))


def get_isonymy_and_migration_indicators_2021(upstream, product):
    DIVISION_KEY = "department_id"

    departamental_df = pandas.merge(
        pandas.merge(
            pandas.read_parquet(str(upstream["get-departmental-karlin-mcgregor-2021"])),
            pandas.read_parquet(str(upstream["get-departmental-isonymy-2021"])).rename(
                columns={"division": DIVISION_KEY}
            ),
            on="department_id",
        ),
        pandas.read_parquet(str(upstream["get-departmental-wright-m-2021"])),
        on="department_id",
    )

    departamental_df.columns = [column.lower() for column in departamental_df.columns]
    col_order = [
        "department_id",
        "n",
        "ins",
        "fst",
        "fishers_alpha",
        "a",
        "b",
        "v",
        "m",
        "population_2021",
    ]

    departamental_df = departamental_df[col_order]
    departamental_df.to_parquet(str(product))
