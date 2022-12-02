import pandas
import geopandas
import matplotlib.pyplot as plt
from matplotlib import colors
import numpy
from matplotlib.patches import Rectangle
from matplotlib.legend_handler import HandlerBase
from matplotlib.patches import Patch
import matplotlib 
import seaborn
from surnames_package import geovis
from surnames_package import utils

def plot_choroplet_compartive_map(upstream, product, departmentShapePath):
    
    # read shape
    department_shp = geopandas.read_file(departmentShapePath)
    # read data
    df_2001 = pandas.read_parquet(str(upstream['get-karlin-mcgregor-departamental-2001']))
    df_2015 = pandas.read_parquet(str(upstream['get-karlin-mcgregor-departamental-2015']))
    df_2021 = pandas.read_parquet(str(upstream['get-karlin-mcgregor-departamental-2021']))

    fig, axes = plt.subplots(ncols=3, figsize=(24, 16))

    data_shp_2001 = pandas.merge(
        department_shp,
        df_2001,
        left_on="departamento_id",
        right_on="department_id"
    )
    data_shp_2015 = pandas.merge(
        department_shp,
        df_2015,
        left_on="departamento_id",
        right_on="department_id"
    )
    data_shp_2021 = pandas.merge(
        department_shp,
        df_2021,
        left_on="departamento_id",
        right_on="department_id"
    )

    max_v = max([
        data_shp_2001.v.max(),
        data_shp_2015.v.max(),
        data_shp_2021.v.max()]
    )
    min_v = min([
        data_shp_2001.v.min(),
        data_shp_2015.v.min(),
        data_shp_2021.v.min()]
    )

    v_cmap = seaborn.diverging_palette(250, 5, as_cmap=True)
    nd_cmap = matplotlib.cm.get_cmap("Greys").copy()

    datasets = [data_shp_2001, data_shp_2015, data_shp_2021]
    year_labels = ['2001', '2015', '2021']
    
    cmaps = [v_cmap, nd_cmap]

    for i, (year, dataset) in enumerate(zip(year_labels, datasets)):
        
        ax_i = axes[i]
        
        department_shp.plot(
            color=nd_cmap.get_over(),
            ax=ax_i
        )

        dataset = dataset.dropna(subset=['v'])
        dataset.plot(
            column="v",
            legend=False,
            vmax=max_v,
            vmin=min_v,
            cmap=v_cmap,
            ax=ax_i
        )

        ax_i.set_axis_off()
        ax_i.set_title(f"{year}", fontsize=28)
        cmap_labels = [
            "Karling MacGregor's v",
            f"No data: {len(department_shp) - len(dataset)}"
        ]

        # create proxy artists as handles:
        cmap_handles = [Rectangle((0, 0), 1, 1) for _ in cmaps]
        handler_map = dict(zip(
            cmap_handles, 
            [
                geovis.HandlerColormap(v_cmap, num_stripes=8),
                geovis.HandlerColormap(nd_cmap, num_stripes=1)
            ])
        )

        ax_i.legend(
            handles=cmap_handles, 
            labels=cmap_labels, 
            handler_map=handler_map, 
            fontsize=12,
            loc='best',
            bbox_to_anchor=(0.5, 0., 0.5, 0.2)
        )

    
    # shared colorbar at the end:
    RedtoBluesIndex = 1
    im = plt.gca().get_children()[RedtoBluesIndex]
    cax = fig.add_axes([0.1,0.05,0.8,0.03])
    bounds = numpy.linspace(min_v, max_v, num=10)
    
    norm = colors.BoundaryNorm(bounds, v_cmap.N)

    fig.colorbar(
        im,
        cax=cax,
        orientation='horizontal',
        ticks=bounds,
        norm=norm,
        spacing='uniform'
    )

    plt.suptitle("Karlin-McGregor's v", fontsize=24)
    plt.savefig(str(product), dpi=300)
    plt.close()


def plot_choroplet_compartive_map_wright_m(upstream, product, departmentShapePath):
    
    # read shape
    department_shp = geopandas.read_file(departmentShapePath)
    # read data
    df_2001 = pandas.read_parquet(str(upstream['get-wright-m-departaments-2001']))
    df_2015 = pandas.read_parquet(str(upstream['get-wright-m-departaments-2015']))
    df_2021 = pandas.read_parquet(str(upstream['get-wright-m-departaments-2021']))
    
    data_shp_2001 = pandas.merge(
        department_shp,
        df_2001,
        left_on="departamento_id",
        right_on="department_id"
    )
    data_shp_2015 = pandas.merge(
        department_shp,
        df_2015,
        left_on="departamento_id",
        right_on="department_id"
    )
    data_shp_2021 = pandas.merge(
        department_shp,
        df_2021,
        left_on="departamento_id",
        right_on="department_id"
    )

    max_m = max([
        data_shp_2001['m'].max(),
        data_shp_2015['m'].max(),
        data_shp_2021['m'].max()]
    )
    min_m = min([
        data_shp_2001['m'].min(),
        data_shp_2015['m'].min(),
        data_shp_2021['m'].min()]
    )

    fig, axes = plt.subplots(ncols=3, figsize=(24, 16))
    m_cmap = seaborn.diverging_palette(250, 5, as_cmap=True)
    nd_cmap = matplotlib.cm.get_cmap("Greys").copy()

    datasets = [data_shp_2001, data_shp_2015, data_shp_2021]
    year_labels = ['2001', '2015', '2021']
    
    cmaps = [m_cmap, nd_cmap]

    for i, (year, dataset) in enumerate(zip(year_labels, datasets)):
        
        ax_i = axes[i]
        
        department_shp.plot(
            color=nd_cmap.get_over(),
            ax=ax_i
        )

        dataset = dataset.dropna(subset=['m'])
        dataset.plot(
            column="m",
            legend=False,
            vmax=max_m,
            vmin=min_m,
            cmap=m_cmap,
            ax=ax_i
        )

        ax_i.set_axis_off()
        ax_i.set_title(f"{year}", fontsize=28)
        cmap_labels = [
            "Wright's m",
            f"No data: {len(department_shp) - len(dataset)}"
        ]

        # create proxy artists as handles:
        cmap_handles = [Rectangle((0, 0), 1, 1) for _ in cmaps]
        handler_map = dict(zip(
            cmap_handles, 
            [
                geovis.HandlerColormap(m_cmap, num_stripes=8),
                geovis.HandlerColormap(nd_cmap, num_stripes=1)
            ])
        )

        ax_i.legend(
            handles=cmap_handles, 
            labels=cmap_labels, 
            handler_map=handler_map, 
            fontsize=12,
            loc='best',
            bbox_to_anchor=(0.5, 0., 0.5, 0.2)
        )

    
    # shared colorbar at the end:
    RedtoBluesIndex = 1
    im = plt.gca().get_children()[RedtoBluesIndex]
    cax = fig.add_axes([0.1,0.05,0.8,0.03])
    bounds = numpy.linspace(min_m, max_m, num=10)
    norm = colors.BoundaryNorm(bounds, m_cmap.N)

    fig.colorbar(
        im,
        cax=cax,
        orientation='horizontal',
        ticks=bounds,
        norm=norm,
        spacing='uniform'
    )

    plt.suptitle("Wright's m", fontsize=24)
    plt.savefig(str(product), dpi=300)
    plt.close()
    
    
def plot_choroplet_comparative_maps(upstream, product, departmentShapePath, region_name, work_column:str = 'm_100'):
    """ Tenemos datasets y shape en donde cada dataset_i comparte con shape un
    atributo de separación de celdas por ejemplo ´region_nombre´.
    
    """
    department_shp = geopandas.read_file(departmentShapePath)
    # read data
    df_2001 = pandas.read_parquet(upstream['get-wright-m-departaments-2001'])
    df_2015 = pandas.read_parquet(upstream['get-wright-m-departaments-2015'])
    df_2021 = pandas.read_parquet(upstream['get-wright-m-departaments-2021'])

    df_2001 = utils.append_cell_codes(df_2001, departmentCodeColumn='department_id')
    df_2015 = utils.append_cell_codes(df_2015, departmentCodeColumn='department_id')
    df_2021 = utils.append_cell_codes(df_2021, departmentCodeColumn='department_id')
    
    # TODO: esto que lo haga get-wright-m-departaments-2021 ...
    df_2001['m_100'] = df_2001['m'] *100
    df_2015['m_100'] = df_2015['m'] *100
    df_2021['m_100'] = df_2021['m'] *100
    
    # regions = ["NOA", "NEA"]
    # for region_name in regions:
    region_shape = department_shp[department_shp['region_nombre'] == region_name]

    # COL = 'm_100'
    region_df_2001 = df_2001[df_2001['region_nombre'] == region_name]
    region_df_2015 = df_2015[df_2015['region_nombre'] == region_name]
    region_df_2021 = df_2021[df_2021['region_nombre'] == region_name]

    region_df_2001 = region_df_2001.dropna(subset=[work_column])
    region_df_2015 = region_df_2015.dropna(subset=[work_column])
    region_df_2021 = region_df_2021.dropna(subset=[work_column])

    datasets = {
        '2001': region_df_2001,
        '2015': region_df_2015,
        '2021': region_df_2021
    }

    f, axs = geovis.plot_comparative_choropleths(
        datasets=datasets,
        shape=region_shape,
        mergeColDataset="departamento_id",
        mergeColShape="department_id",
        plotColumn=work_column,
        gradientLabel=f"{work_column} value"
    )

    title_ypositions = {
        "NOA": 0.93,
        "NEA": 0.86,
        "Centro": 0.91,
        "Cuyo":1.05,
        "Patagonia":1.05
    }

    f.suptitle(
        region_name,
        y=title_ypositions[region_name],
        fontsize=28
    )

    f.savefig(str(product), dpi=300)
    plt.close()
