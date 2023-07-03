import pandas
from surnames_package import isonymic
from surnames_package import utils

DF_COLUMNS = ['division', 'n', 'ins', 'fst', 'fishers_alpha', 'A', 'B' ]

def get_regional_isonymy_2015(product):
    """"""
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet")
    
    df = utils.append_province_description(df)
    regional_results = []

    for (region_name, region_df) in df.groupby('region_nombre'):
        
        region_isonymy = isonymic.get_isonymic_data(region_df['surname'])
        region_isonymy['division'] = region_name
        regional_results.append(region_isonymy)
    
    regional_df = pandas.DataFrame(regional_results)
    regional_df = regional_df[DF_COLUMNS]
    
    regional_df.to_parquet(str(product))