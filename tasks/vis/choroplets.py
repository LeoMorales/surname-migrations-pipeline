import pandas
import geopandas
import matplotlib.pyplot as plt
import matplotlib.cm as colormap
from matplotlib import colors
import numpy as np
from matplotlib.patches import Rectangle
from matplotlib.legend_handler import HandlerBase
from matplotlib.patches import Patch
import matplotlib 
import seaborn

class HandlerColormap(HandlerBase):
    def __init__(self, cmap, num_stripes=8, **kw):
        HandlerBase.__init__(self, **kw)
        self.cmap = cmap
        self.num_stripes = num_stripes

    def create_artists(self, legend, orig_handle, 
                       xdescent, ydescent, width, height, fontsize, trans):
        stripes = []
        
        if self.num_stripes == 1:
            # solo un cuadradito con el último color del cmap
            stripes.append(
                Rectangle(
                    [0, 0], 
                    width, 
                    height, 
                    fc=self.cmap.get_over(), 
                    transform=trans
                )
            )
        else:
            for i in range(self.num_stripes):
                s = Rectangle(
                    [xdescent + i * width / self.num_stripes, ydescent], 
                    width / self.num_stripes, 
                    height, 
                    fc=self.cmap((2 * i + 1) / (2 * self.num_stripes)), 
                    transform=trans
                )
                stripes.append(s)
        
        return stripes

def plot_choroplet_compartive_map(upstream, product, departmentShapePath):
    
    # read shape
    department_shp = geopandas.read_file(departmentShapePath)
    # read data
    df_2000 = pandas.read_parquet(str(upstream['get-karlin-mcgregor-departamental-2000']))
    df_2015 = pandas.read_parquet(str(upstream['get-karlin-mcgregor-departamental-2015']))
    df_2021 = pandas.read_parquet(str(upstream['get-karlin-mcgregor-departamental-2021']))

    fig, axes = plt.subplots(ncols=3, figsize=(24, 16))

    data_shp_2000 = pandas.merge(
        department_shp,
        df_2000,
        on="departamento_id"
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
        data_shp_2000.v.max(),
        data_shp_2015.v.max(),
        data_shp_2021.v.max()]
    )
    min_v = min([
        data_shp_2000.v.min(),
        data_shp_2015.v.min(),
        data_shp_2021.v.min()]
    )

    v_cmap = seaborn.diverging_palette(250, 5, as_cmap=True)
    nd_cmap = matplotlib.cm.get_cmap("Greys").copy()

    datasets = [data_shp_2000, data_shp_2015, data_shp_2021]
    year_labels = ['2000', '2015', '2021']
    
    cmaps = [v_cmap, nd_cmap]

    for i, (year, dataset) in enumerate(zip(year_labels, datasets)):
        
        ax_i = axes[i]
        
        department_shp.plot(
            color=nd_cmap.get_over(),
            ax=ax_i
        )

        dataset = dataset.dropna(subset=['v'])
        map = dataset.plot(
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
                HandlerColormap(v_cmap, num_stripes=8),
                HandlerColormap(nd_cmap, num_stripes=1)
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
    bounds = [0, 0.025, .05, .1, .2]
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