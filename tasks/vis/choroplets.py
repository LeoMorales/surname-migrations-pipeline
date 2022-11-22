import pandas
import geopandas
import matplotlib.pyplot as plt
import matplotlib.cm as colormap
from matplotlib import colors


def plot_choroplet_compartive_map(upstream, product, departmentShapePath):
    
    # read shape
    department_shp = geopandas.read_file(departmentShapePath)
    # read data
    df_2015 = pandas.read_parquet(str(upstream['get-karlin-mcgregor-departamental-2015']))
    df_2021 = pandas.read_parquet(str(upstream['get-karlin-mcgregor-departamental-2021']))

    fig, axes = plt.subplots(ncols=2, figsize=(20, 16))

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

    max_v = data_shp_2015.v.max() if data_shp_2015.v.max() > data_shp_2021.v.max() else data_shp_2021.v.max()
    min_v = data_shp_2015.v.min() if data_shp_2015.v.min() < data_shp_2021.v.min() else data_shp_2021.v.min()

    # legend_kwds={'shrink': 0.9, 'pad':0.2},
    data_shp_2015.plot(
        column="v",
        legend=False,
        vmax=max_v,
        vmin=min_v,
        ax=axes[0]
    )

    axes[0].set_axis_off()
    axes[0].set_title("2015", fontsize=28)

    data_shp_2021.plot(
        column="v",
        legend=False,
        vmax=max_v,
        vmin=min_v,
        ax=axes[1]
    )

    axes[1].set_axis_off()
    axes[1].set_title("2021", fontsize=30)

    # shared colorbar at the end:
    im = plt.gca().get_children()[0]
    cax = fig.add_axes([0.1,0.05,0.8,0.03])
    cmap = colormap.viridis
    bounds = [0, 0.025, .05, .1, .2]
    norm = colors.BoundaryNorm(bounds, cmap.N)

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