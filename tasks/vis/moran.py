import pandas
import geopandas
import logging

from surnames_package import spatial
from surnames_package import spatial_vis
from surnames_package import utils
import matplotlib.pyplot as plt

NEIGHBORHOOD_STRATEGY = 'knn'
NEIGHBORHOOD_PARAM = 8

def plot_moran_clustermap_2001_2021(upstream, product, departmentShapePath, regionName, analysisColumn):
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # read data
    df_2001 = pandas.read_parquet(str(upstream['combine-departamental-datasets']['data_2001']))
    df_2015 = pandas.read_parquet(str(upstream['combine-departamental-datasets']['data_2015']))
    df_2021 = pandas.read_parquet(str(upstream['combine-departamental-datasets']['data_2021']))
    
    # append region name
    df_2001 = utils.append_cell_description(df_2001, departmentCodeColumn="department_id")
    df_2015 = utils.append_cell_description(df_2015, departmentCodeColumn="department_id")
    df_2021 = utils.append_cell_description(df_2021, departmentCodeColumn="department_id")
    
    # logger.info(f'df_2001 len: {len(df_2001)}')
    # logger.info(f'df_2015 len: {len(df_2015)}')
    # logger.info(f'df_2021 len: {len(df_2021)}')
    
    shape = geopandas.read_file(departmentShapePath)
        
    datasets = {
        '2001': df_2001,
        '2015': df_2015,
        '2021': df_2021
    }

    N_COLS = len(datasets)
    fig, axes = plt.subplots(
        ncols=N_COLS,
        figsize=(8 * N_COLS, 10)
    )
    axes = axes.flatten()

    for ax, year in zip(axes, datasets.keys()):
        dataset = datasets[year]
        regional_shape = shape

        if regionName != 'Argentina':
            dataset = dataset[dataset['region_nombre'] == regionName]
            regional_shape = shape[shape['region_nombre'] == regionName]
    
        # [ ] - Combinar datos y capa
        geodata = pandas.merge(
            regional_shape,
            dataset,
            left_on="departamento_id",
            right_on="department_id",
            how='inner'
        )

        # [ ] - Calcular Moran global y locales
        weights, moran, lisa = spatial.get_spatials(
            geodata,
            attribute=analysisColumn,
            strategy=NEIGHBORHOOD_STRATEGY,
            k_neighbours=NEIGHBORHOOD_PARAM
        )

        geodata_with_labels = pandas.merge(
            geodata,
            spatial_vis.assign_cluster_label(
                geodata[['departamento_id']],
                lisa,
                SIGNIFICANCE_LIMIT=0.05,
                cellColumn='departamento_id',
                outputColumn='label'
            )
        )

        # [ ] Armar el título
        moran_I = moran.I
        moran_p_sim = moran.p_sim
        figure_title = (f'''
            {year}
            Moran I: {moran_I:0.2f} p-value:{moran_p_sim}'''
        )


        map_ax = spatial_vis.create_clustermap_figure(
            geodata_with_labels, regional_shape, figure_title, output_path="", ax=ax)

        # (left, bottom, width, height)
        bbox_to_anchor = (1.25, .4, 0, 0)

        if regionName in ['Centro', 'Cuyo']:
            # bajar un poquito la leyenda
            # (left, bottom, width, height)
            bbox_to_anchor = (1.2, 0.2, 0, 0)
        if regionName in ['Patagonia']:
            # (left, bottom, width, height)
            bbox_to_anchor = (1.55, .5, 0, 0)
        if regionName in ['NEA', 'NOA']:
            # (left, bottom, width, height)
            bbox_to_anchor = (1.3, .15, 0, 0)
        
        map_ax.get_legend().set_bbox_to_anchor(bbox_to_anchor)

    fig.suptitle(
        f"Moran cluster map: {regionName} | Indicator: {analysisColumn}",
        fontsize=24,
        horizontalalignment="right",
        x=0.4)
    plt.savefig(str(product))
    plt.close()