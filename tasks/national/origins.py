import pandas


def assign_origin_tag_to_popular_surnames_2015(upstream, product):
    """Asigna una etiqueta de origen a cada uno de los diez apellidos que mas cantidad de portadores tiene en argentina

    Args:
        upstream (_type_): _description_
        product (_type_):
            Data columns (total 7 columns):
            #   Column                   Non-Null Count  Dtype
            ---  ------                   --------------  -----
            0   surname                  10 non-null     object
            1   bearers                  10 non-null     int64
            2   division_bearer_ranking  10 non-null     int64
            3   division_level           10 non-null     object
            4   division_id              10 non-null     object
            5   year                     10 non-null     object
            6   classification           10 non-null     object
            dtypes: int64(2), object(5)
    """
    df = pandas.read_parquet(
        str(
            upstream["get-national-bearers-rankings-2015"][
                "surnames-with-highest-number-of-bearers"
            ]
        )
    )

    labels_df = pandas.read_parquet(str(upstream["get-origin-labels"]))

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
            0   surname                  10 non-null     object
            1   bearers                  10 non-null     int64
            2   division_bearer_ranking  10 non-null     int64
            3   division_level           10 non-null     object
            4   division_id              10 non-null     object
            5   year                     10 non-null     object
            6   classification           10 non-null     object
            dtypes: int64(2), object(5)
    """
    df = pandas.read_parquet(
        str(
            upstream["get-national-bearers-rankings-2021"][
                "surnames-with-highest-number-of-bearers"
            ]
        )
    )

    labels_df = pandas.read_parquet(str(upstream["get-origin-labels"]))

    df = df.merge(labels_df[["surname", "classification"]], on="surname", how="left")

    df["classification"] = df["classification"].fillna("SIN-CLASIFICAR")
    df.to_parquet(str(product))
