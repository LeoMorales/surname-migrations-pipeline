import pandas
import geopandas
import matplotlib.pyplot as plt

from surnames_package import geovis


def plot_comparative_choropleth_maps(
    upstream,
    product,
    provincialShapePath,
    regionalView: str = "Argentina",
    plotColumn: str = "fishers_alpha",
):
    """
    Dibuja un mapa por cada año para la columna especificada.
    """
    provinces_shp = geopandas.read_file(provincialShapePath, driver="GeoJSON")
    # read data
    # df_2001 = pandas.read_parquet(str(upstream["get-merged-indicators-2001"]))
    df_2015 = pandas.read_parquet(
        str(upstream["get-provincial-merged-indicators-2015"])
    )
    df_2021 = pandas.read_parquet(
        str(upstream["get-provincial-merged-indicators-2021"])
    )

    # cuando se combina con la capa, se agrega el campo de region
    gdf_2015 = pandas.merge(
        provinces_shp, df_2015, left_on="provincia_id", right_on="province_id"
    )
    gdf_2021 = pandas.merge(
        provinces_shp, df_2021, left_on="provincia_id", right_on="province_id"
    )
    # regionalView = ["NOA", "NEA", "Argentina"]
    if regionalView == "Argentina":
        plot_shape = provinces_shp
        plot_gdf_2015 = gdf_2015
        plot_gdf_2021 = gdf_2021
    else:
        plot_shape = provinces_shp[provinces_shp["region_indec"] == regionalView]
        plot_gdf_2015 = gdf_2015[gdf_2015["region_indec"] == regionalView]
        plot_gdf_2021 = gdf_2021[gdf_2021["region_indec"] == regionalView]

    plot_gdf_2015 = plot_gdf_2015.dropna(subset=[plotColumn])
    plot_gdf_2021 = plot_gdf_2021.dropna(subset=[plotColumn])

    datasets_info = [
        {
            "dataset": plot_gdf_2015,
            "map_title": "2015",
        },
        {
            "dataset": plot_gdf_2021,
            "map_title": "2021",
        },
    ]

    f, axs = geovis.plot_comparative_choropleth_maps(
        plotDatasets=datasets_info,
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
