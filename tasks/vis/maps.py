import pandas
import geopandas
import matplotlib.pyplot as plt
import seaborn

from surnames_package import utils

FISHERS_ALPHA_COLUMN = "fishers_alpha"
# temp, separar tareas...
def plot_karlin_mcgregor_2015(product, upstream, shape_path):
    """ Dibuja un mapa de la Argentina departamental pintando segun el valor
    de Alfa de Fisher; otro mapa pintando según el valor de v de Karlin-Mcgregor y 
    un scatterplot de alpha y v.
    """
    # shape = geopandas.read_file("/home/lmorales/work/pipelines/resources/departamentos.geojson")
    shape = geopandas.read_file(shape_path)
    data_df = pandas.read_parquet(str(upstream['combine-departamental-datasets']['data_2015']))
    shape_with_data = pandas.merge(
        shape,
        data_df,
        left_on="departamento_id",
        right_on="department_id")

    f, ax = plt.subplots(figsize=(18, 12))
    shape_with_data.plot(column="v", ax=ax, legend=True)
    ax.set_axis_off()
    ax.set_title("Karlin-McGregor's v")
    
    print("Saving: {} ...".format(str(product['map-v'])))
    plt.savefig(str(product['map-v']), dpi=300)
    plt.close()
    
    f, ax = plt.subplots(figsize=(18, 12))
    shape_with_data.plot(column=FISHERS_ALPHA_COLUMN, ax=ax, legend=True, cmap="viridis")
    ax.set_axis_off()
    ax.set_title("Fisher's alpha")
    plt.savefig(str(product['map-a']))
    plt.close()
    
    shape_with_data['region'] = shape_with_data.provincia_id.apply(lambda i: utils.REGION_BY_PROVINCE_CODE[i])
    seaborn.set_theme(style="whitegrid")
    g = seaborn.relplot(
        data=shape_with_data,
        x=FISHERS_ALPHA_COLUMN, y="v",
        hue="region", size="n",
        sizes=(50, 500)
    )
    g.set(xscale="log", yscale="log")
    g.ax.xaxis.grid(True, "minor", linewidth=.25)
    g.ax.yaxis.grid(True, "minor", linewidth=.25)
    g.despine(left=True, bottom=True)
    g.fig.set_size_inches(12,12)
    plt.savefig(str(product['scatterplot-a-and-v']))
    plt.close()

