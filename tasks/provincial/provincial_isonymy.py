import pandas
from surnames_package import isonymic
from surnames_package import utils

DF_COLUMNS = ['division', 'n', 'ins', 'fst', 'fishers_alpha', 'A', 'B' ]

def get_provincial_isonymy_2015(product):
    """"""
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet")
    df = utils.append_province_description(df)
    
    provincial_results = []
    for (province_id, province_df) in df.groupby('province_id'):
        
        province_isonymy = isonymic.get_isonymic_data(province_df['surname'])
        province_isonymy['division'] = province_id
        
        provincial_results.append(province_isonymy)
    
    provincial_df = pandas.DataFrame(provincial_results)
    provincial_df = provincial_df[DF_COLUMNS]
    
    provincial_df.to_parquet(str(product))