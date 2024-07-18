import pandas
import geopandas
import logging

from surnames_package import spatial
from surnames_package import spatial_vis
from surnames_package import utils
import matplotlib.pyplot as plt

neighborhood_strategy_param = "queen"
neighborhood_k_param = 8


def plot_moran_clustermap_2001_2021(
    upstream,
    product,
    departmentShapePath,
    regionName,
    analysisColumn,
    population_threshold: int = 1000,
):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # read data
    # df_2001 = pandas.read_parquet(str(upstream['combine-departamental-datasets']['data_2001']))
    # df_2015 = pandas.read_parquet(str(upstream['combine-departamental-datasets']['data_2015']))
    # df_2021 = pandas.read_parquet(str(upstream['combine-departamental-datasets']['data_2021']))
    df_2001 = pandas.read_parquet(str(upstream["get-merged-indicators-2001"]))
    df_2015 = pandas.read_parquet(str(upstream["get-merged-indicators-2015"]))
    df_2021 = pandas.read_parquet(str(upstream["get-merged-indicators-2021"]))
    df_2001 = df_2001[df_2001["population_2001"] > population_threshold].copy()
    df_2015 = df_2015[df_2015["population_2015"] > population_threshold].copy()
    df_2021 = df_2021[df_2021["population_2021"] > population_threshold].copy()

    # append region name
    df_2001 = utils.append_cell_description(
        df_2001, departmentCodeColumn="department_id"
    )
    df_2015 = utils.append_cell_description(
        df_2015, departmentCodeColumn="department_id"
    )
    df_2021 = utils.append_cell_description(
        df_2021, departmentCodeColumn="department_id"
    )

    shape = geopandas.read_file(departmentShapePath)

    datasets = {"2001": df_2001, "2015": df_2015, "2021": df_2021}

    N_COLS = len(datasets)
    fig, axes = plt.subplots(ncols=N_COLS, figsize=(8 * N_COLS, 10))
    axes = axes.flatten()

    neighborhood_strategy_param = "knn"
    for ax, year in zip(axes, datasets.keys()):
        dataset = datasets[year]
        regional_shape = shape

        if regionName != "Argentina":
            dataset = dataset[dataset["region_nombre"] == regionName]
            regional_shape = shape[shape["region_nombre"] == regionName]

        # [ ] - Combinar datos y capa
        geodata = pandas.merge(
            regional_shape,
            dataset,
            left_on="departamento_id",
            right_on="department_id",
            how="inner",
        )

        # [ ] - Calcular Moran global y locales
        weights, moran, lisa = spatial.get_spatials(
            geodata,
            attribute=analysisColumn,
            strategy=neighborhood_strategy_param,
            k_neighbours=neighborhood_k_param,
        )

        SIGNIFICANCE_LIMIT = 0.05
        # asignar etiquetas de moran local a cada departamento
        quadfilter = (lisa.p_sim <= (SIGNIFICANCE_LIMIT)) * (lisa.q)
        spot_labels = ["NS", "HH", "LH", "LL", "HL"]
        moran_lisa_labels = [spot_labels[i] for i in quadfilter]
        geodata["moran_lisa_label"] = moran_lisa_labels

        no_data_shape = regional_shape[
            ~regional_shape["departamento_id"].isin(geodata["department_id"].unique())
        ]

        spatial_vis.draw_clustermap(
            dataShape=geodata,
            noDataShape=no_data_shape,
            figureTitle=f"Moran Lisa Clustermap {year}\nMoran's I: {moran.I:0.2f} p-value: {moran.p_sim:0.3f}",
            moranLisaColumn="moran_lisa_label",
            tileFilepath="/home/lmorales/resources/contextly/argentina.tif",
            ax=ax,
            plot_kwargs={
                "tranparency_dict": {
                    "NS": 0.4,
                    "HH": 0.75,
                    "LL": 0.75,
                    "HL": 0.8,
                    "LH": 0.99,
                },
                "linewidth_dict": {
                    "NS": 0.99,
                    "HH": 0.9,
                    "LL": 0.9,
                    "HL": 1.5,
                    "LH": 1.5,
                },
            },
        )
        # (left, bottom, width, height)
        bbox_to_anchor = (1.25, 0.4, 0, 0)

        if regionName in ["Centro", "Cuyo"]:
            # bajar un poquito la leyenda
            # (left, bottom, width, height)
            bbox_to_anchor = (1.2, 0.2, 0, 0)
        if regionName in ["Patagonia"]:
            # (left, bottom, width, height)
            bbox_to_anchor = (1.55, 0.5, 0, 0)
        if regionName in ["NEA", "NOA"]:
            # (left, bottom, width, height)
            bbox_to_anchor = (1.3, 0.15, 0, 0)

        # map_ax.get_legend().set_bbox_to_anchor(bbox_to_anchor)

    fig.suptitle(
        f"{regionName} | {analysisColumn} | '{neighborhood_strategy_param}'",
        fontsize=24,
        horizontalalignment="center",
        x=0.4,
    )
    plt.savefig(str(product))
    plt.close()
