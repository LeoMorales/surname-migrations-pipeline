import pandas
import geopandas
import matplotlib.pyplot as plt

from surnames_package import geovis
from surnames_package import utils


def plot_comparative_choropleth_maps(
    upstream,
    product,
    departmentShapePath,
    regionalView: str = "Argentina",
    plotColumn: str = "fishers_alpha",
):
    """
    Dibuja un mapa por cada año para la columna especificada.
    """
    department_shp = geopandas.read_file(departmentShapePath)
    # read data
    df_2001 = pandas.read_parquet(str(upstream["get-merged-indicators-2001"]))
    df_2015 = pandas.read_parquet(str(upstream["get-merged-indicators-2015"]))
    df_2021 = pandas.read_parquet(str(upstream["get-merged-indicators-2021"]))

    # cuando se combina con la capa, se agrega el campo de region
    gdf_2001 = pandas.merge(
        department_shp, df_2001, left_on="departamento_id", right_on="department_id"
    )
    gdf_2015 = pandas.merge(
        department_shp, df_2015, left_on="departamento_id", right_on="department_id"
    )
    gdf_2021 = pandas.merge(
        department_shp, df_2021, left_on="departamento_id", right_on="department_id"
    )
    # regionalView = ["NOA", "NEA", "Argentina"]
    if regionalView == "Argentina":
        plot_shape = department_shp
        plot_gdf_2001 = gdf_2001
        plot_gdf_2015 = gdf_2015
        plot_gdf_2021 = gdf_2021
    else:
        plot_shape = department_shp[department_shp["region_nombre"] == regionalView]
        plot_gdf_2001 = gdf_2001[gdf_2001["region_nombre"] == regionalView]
        plot_gdf_2015 = gdf_2015[gdf_2015["region_nombre"] == regionalView]
        plot_gdf_2021 = gdf_2021[gdf_2021["region_nombre"] == regionalView]

    plot_gdf_2001 = plot_gdf_2001.dropna(subset=[plotColumn])
    plot_gdf_2015 = plot_gdf_2015.dropna(subset=[plotColumn])
    plot_gdf_2021 = plot_gdf_2021.dropna(subset=[plotColumn])

    datasets_info = [
        {
            "dataset": plot_gdf_2001.rename(columns={"population_2001": "population"}),
            "map_title": "2001",
        },
        {
            "dataset": plot_gdf_2015.rename(columns={"population_2015": "population"}),
            "map_title": "2015",
        },
        {
            "dataset": plot_gdf_2021.rename(columns={"population_2021": "population"}),
            "map_title": "2021",
        },
    ]

    f, axs = geovis.plot_comparative_choropleth_maps(
        datasets_info=datasets_info,
        shape=plot_shape,
        plotColumn=plotColumn,
        gradientLabel=f"{plotColumn} value",
    )

    title_ypositions = {
        "NOA": 0.93,
        "NEA": 0.86,
        "Centro": 0.91,
        "Cuyo": 0.95,
        "Patagonia": 0.95,
    }

    f.suptitle(
        f"Spatial trend of the value of {plotColumn} in {regionalView}",
        y=title_ypositions.get(regionalView, 0.95),
        fontsize=28,
    )

    f.savefig(str(product), dpi=300)
    plt.close()
