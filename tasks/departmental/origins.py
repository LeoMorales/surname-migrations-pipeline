import pandas


def assign_origin_tag_to_popular_surnames_2015(upstream, product):
    """Asigna una etiqueta de origen a cada uno de los diez apellidos que mas cantidad de portadores tiene en argentina

    Args:
        upstream (_type_): _description_
        product (_type_):
            Data columns (total 7 columns):
            #   Column                   Non-Null Count  Dtype
            ---  ------                   --------------  -----
            0   surname                  5270 non-null   object
            1   bearers                  5270 non-null   int64
            2   division_bearer_ranking  5270 non-null   int64
            3   division_level           5270 non-null   object
            4   division_id              5270 non-null   object
            5   year                     5270 non-null   object
            6   classification           5270 non-null   object
            dtypes: int64(2), object(5)
    """
    df = pandas.read_parquet(
        str(
            upstream["get-departmental-bearers-rankings-2015"][
                "surnames-with-highest-number-of-bearers"
            ]
        )
    )

    labels_df = pandas.read_parquet(str(upstream["get-origin-labels"]))
    labels_df["classification"] = labels_df["classification"].str.replace(
        "SIN CLASIFICAR", "SIN-CLASIFICAR"
    )

    df = df.merge(labels_df[["surname", "classification"]], on="surname", how="left")

    df["classification"] = df["classification"].fillna("SIN-CLASIFICAR")
    df.to_parquet(str(product))


def assign_origin_tag_to_popular_surnames_2021(upstream, product):
    """Asigna una etiqueta de origen a cada uno de los diez apellidos que mas cantidad de portadores tiene en argentina

    Args:
        upstream (_type_): _description_
        product (_type_):
            Data columns (total 7 columns):
            #   Column                   Non-Null Count  Dtype
            ---  ------                   --------------  -----
            0   surname                  5270 non-null   object
            1   bearers                  5270 non-null   int64
            2   division_bearer_ranking  5270 non-null   int64
            3   division_level           5270 non-null   object
            4   division_id              5270 non-null   object
            5   year                     5270 non-null   object
            6   classification           5270 non-null   object
            dtypes: int64(2), object(5)
    """
    df = pandas.read_parquet(
        str(
            upstream["get-departmental-bearers-rankings-2021"][
                "surnames-with-highest-number-of-bearers"
            ]
        )
    )

    labels_df = pandas.read_parquet(str(upstream["get-origin-labels"]))
    labels_df["classification"] = labels_df["classification"].str.replace(
        "SIN CLASIFICAR", "SIN-CLASIFICAR"
    )

    df = df.merge(labels_df[["surname", "classification"]], on="surname", how="left")

    df["classification"] = df["classification"].fillna("SIN-CLASIFICAR")
    df.to_parquet(str(product))


def assign_origin_label_to_single_bearer_surnames_2015(upstream, product):
    """Asigna una etiqueta de origen a aquellos apellidos que tienen un único portador en cada departamento de la Argentina.

    Args:
        upstream (_type_): Input
        product (_type_): Output
    """
    df_one_bearer = pandas.read_parquet(
        str(
            upstream["get-departmental-bearers-rankings-2015"][
                "surnames-with-only-one-bearer"
            ]
        )
    )
    labels_df = pandas.read_parquet(str(upstream["get-origin-labels"]))
    labels_df["classification"] = labels_df["classification"].str.replace(
        "SIN CLASIFICAR", "SIN-CLASIFICAR"
    )

    # Define a regular expression pattern to match any unwanted characters
    unwanted_characters_pattern = r"[ç&@#?!]"

    # print(len(df_one_bearer))
    # Use str.contains to filter out rows with unwanted characters
    df_one_bearer = df_one_bearer[
        ~df_one_bearer["surname"].str.contains(unwanted_characters_pattern, regex=True)
    ]

    # print(len(df_one_bearer))

    df_output = pandas.merge(
        df_one_bearer, labels_df[["surname", "classification"]], on="surname"
    )

    df_output.to_parquet(str(product))


def assign_origin_label_to_single_bearer_surnames_2021(upstream, product):
    """Asigna una etiqueta de origen a aquellos apellidos que tienen un único portador en cada departamento de la Argentina.

    Args:
        upstream (_type_): Input
        product (_type_): Output
    """
    df_one_bearer = pandas.read_parquet(
        str(
            upstream["get-departmental-bearers-rankings-2021"][
                "surnames-with-only-one-bearer"
            ]
        )
    )

    labels_df = pandas.read_parquet(str(upstream["get-origin-labels"]))
    labels_df["classification"] = labels_df["classification"].str.replace(
        "SIN CLASIFICAR", "SIN-CLASIFICAR"
    )

    # Define a regular expression pattern to match any unwanted characters
    unwanted_characters_pattern = r"[ç&@#?!]"

    # Use str.contains to filter out rows with unwanted characters
    df_one_bearer = df_one_bearer[
        ~df_one_bearer["surname"].str.contains(unwanted_characters_pattern, regex=True)
    ]

    df_output = pandas.merge(
        df_one_bearer, labels_df[["surname", "classification"]], on="surname"
    )

    df_output.to_parquet(str(product))
