from surnames_package import paths

# +
import geopandas

shape_path = '/home/lmorales/work/pipelines/resources/departamentos.geojson'
department_shp = geopandas.read_file(shape_path)
# -

department_shp.head()

import pandas

df_2015 = pandas.read_parquet("../_products/karlin-mcgregor/karlin-mcgregor-departamental-2015.parquet")

df_2015.head()

# +
df_2021 = pandas.read_parquet("../_products/karlin-mcgregor/karlin-mcgregor-departamental-2021.parquet")

print(df_2021.head())
# -

len(df_2021.department_id.unique())

len(df_2015.department_id.unique())



import matplotlib.pyplot as plt

max_a = df_2015.a.max() if df_2015.a.max() > df_2021.a.max() else df_2021.a.max()
min_a = df_2015.a.min() if df_2015.a.min() < df_2021.a.min() else df_2021.a.min()
print(min_a, max_a)

import matplotlib.pyplot as plt
import matplotlib.cm as colormap
from matplotlib import colors

# +
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
print(min_v, max_v)


data_shp_2015.plot(
    column="v",
    legend=True,
    legend_kwds={'shrink': 0.3},
    vmax=max_v,
    vmin=min_v,
    ax=axes[0]
)

#axes[0].set_xlim(-59, -57)
#axes[0].set_ylim(-36, -34)
axes[0].set_axis_off()
axes[0].set_title("2015")

data_shp_2021.plot(
    column="v",
    legend=True,
    legend_kwds={'shrink': 0.3},
    vmax=max_v,
    vmin=min_v,
    ax=axes[1]
)

axes[1].set_axis_off()
axes[1].set_title("2021")

plt.suptitle("Karlin-McGregor's v\n2015 & 2021", fontsize=24)

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


plt.show();

# +
fig, axes = plt.subplots(ncols=2, figsize=(20, 16))

max_a = data_shp_2015.a.max() if data_shp_2015.a.max() > data_shp_2021.a.max() else data_shp_2021.a.max()
min_a = data_shp_2015.a.min() if data_shp_2015.a.min() < data_shp_2021.a.min() else data_shp_2021.a.min()
print(min_a, max_a)


data_shp_2015.plot(
    column="a",
    legend=True,
    legend_kwds={'shrink': 0.3},
    vmax=max_a,
    vmin=min_a,
    ax=axes[0]
)

#axes[0].set_xlim(-59, -57)
#axes[0].set_ylim(-36, -34)
axes[0].set_axis_off()
axes[0].set_title("2015")

data_shp_2021.plot(
    column="a",
    legend=True,
    legend_kwds={'shrink': 0.3},
    vmax=max_a,
    vmin=min_a,
    ax=axes[1]
)

axes[1].set_axis_off()
axes[1].set_title("2021")

plt.suptitle("Fisher's alpha\n2015 & 2021", fontsize=24, ha='right')
plt.show();
# -
data_shp_2021.head()


import seaborn

seaborn.displot(
    data=data_shp_2021, x='a', kind='hist',
    height=6, aspect=1.4, bins=18,
    hue='region_indec'
)

seaborn.displot(data_shp_2015, x="a", kind="kde", hue="region_indec", multiple="stack")

seaborn.displot(data_shp_2021, x="a", kind="kde", hue="region_indec", multiple="stack")

cols = ["departamento_id", "region_indec", "v"]
cols = ["v", "a"]

regions = data_shp_2015.region_indec.unique()

print(regions)

# +
f, ax = plt.subplots(3, 2, figsize=(12,16))

axes = ax.flatten()
for i, region_name_i in enumerate(regions):
    
    data_2015 = data_shp_2015[data_shp_2015.region_indec == region_name_i][cols].assign(year="2015").copy()
    data_2021 = data_shp_2021[data_shp_2021.region_indec == region_name_i][cols].assign(year="2021").copy()
    data = pandas.concat([data_2015, data_2021])
    
    axis = axes[i]
    seaborn.kdeplot(data=data, x="v", hue="year", multiple="stack", ax= axis)
    
    # Hide the right and top spines
    axis.spines.right.set_visible(False)
    axis.spines.top.set_visible(False)
    
    axis.set_title(f"Region: {region_name_i}")
    
axes[-1].remove()
plt.suptitle("Karlin-McGregor's v\nRegional plots", y=.95)
plt.show()

# +
f, ax = plt.subplots(3, 2, figsize=(12,16))

axes = ax.flatten()
for i, region_name_i in enumerate(regions):
    
    data_2015 = data_shp_2015[data_shp_2015.region_indec == region_name_i][cols].assign(year="2015").copy()
    data_2021 = data_shp_2021[data_shp_2021.region_indec == region_name_i][cols].assign(year="2021").copy()
    data = pandas.concat([data_2015, data_2021])
    
    axis = axes[i]
    seaborn.kdeplot(data=data, x="a", hue="year", multiple="stack", ax= axis)
    
    # Hide the right and top spines
    axis.spines.right.set_visible(False)
    axis.spines.top.set_visible(False)
    
    axis.set_title(f"Region: {region_name_i}")
    
axes[-1].remove()
plt.suptitle("Fisher's alpha\nRegional plots", y=.95)
plt.show()
# -

data_shp_2015.head()

seaborn.scatterplot(data=data_shp_2015, x="n", y="a", hue="region_indec")

seaborn.scatterplot(data=data_shp_2015, x="n", y="v", hue="region_indec")

data_shp_2015.eval("a / n")

import math

data_shp_2015.a.apply(math.log)


