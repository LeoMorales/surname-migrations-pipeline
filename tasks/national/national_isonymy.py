import pandas
from surnames_package import isonymic

DF_COLUMNS = ["division", "n", "ins", "fst", "fishers_alpha", "A", "B"]


def get_national_isonymy_2015(upstream, product):
    """"""
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))

    isonymic_result = isonymic.get_isonymic_data(df["surname"])
    data = [["Argentina"] + list(isonymic_result.values())]
    columns = ["division"] + list(isonymic_result.keys())

    output_df = pandas.DataFrame(data=data, columns=columns)
    output_df.to_parquet(str(product))


def get_national_bearers_rankings_2015(upstream, product):
    """Obtiene los rankings de apellidos con mas cantidad de portadores y el ranking (top1) de los apellidos con un solo portador.

    Args:
        upstream (_type_): _description_
        product (_type_): _description_
    """
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))

    (
        highest_number_of_bearers,
        only_one_bearer,
    ) = isonymic.getSurnamesWithTheHighestNumberOfBearersAndThoseWithOnlyOneBearer(
        df["surname"]
    )

    highest_number_of_bearers["year"] = "2015"
    only_one_bearer["year"] = "2015"

    highest_number_of_bearers.to_parquet(
        str(product["surnames-with-highest-number-of-bearers"])
    )
    only_one_bearer.to_parquet(str(product["surnames-with-only-one-bearer"]))


def get_national_bearers_rankings_2021(upstream, product):
    """Obtiene los rankings de apellidos con mas cantidad de portadores y el ranking (top1) de los apellidos con un solo portador.

    Args:
        upstream (_type_): _description_
        product (_type_): _description_
    """
    df = pandas.read_parquet(str(upstream["get-surnames-2021"]))

    (
        highest_number_of_bearers,
        only_one_bearer,
    ) = isonymic.getSurnamesWithTheHighestNumberOfBearersAndThoseWithOnlyOneBearer(
        df["surname"]
    )

    highest_number_of_bearers["year"] = "2021"
    only_one_bearer["year"] = "2021"

    highest_number_of_bearers.to_parquet(
        str(product["surnames-with-highest-number-of-bearers"])
    )
    only_one_bearer.to_parquet(str(product["surnames-with-only-one-bearer"]))
