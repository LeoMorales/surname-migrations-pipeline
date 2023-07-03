import pandas

def get_departamental__moran_clustermap(upstream, product):
    data = pandas.read_parquet(str(upstream["get-departmental-isonymy-2015"]))