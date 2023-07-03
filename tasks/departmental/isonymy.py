import pandas
from surnames_package import isonymic
from surnames_package import utils

DF_COLUMNS = ['division', 'n', 'ins', 'fst', 'fishers_alpha', 'A', 'B' ]

def get_departmental_isonymy_2015(product):
    """"""
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet")
    #df = utils.append_province_description(df)
    
    departamental_results = []

    for (departament_id, department_df) in df.groupby('department_id'):
        
        department_isonymy = isonymic.get_isonymic_data(department_df['surname'])
        department_isonymy['division'] = departament_id
        
        departamental_results.append(department_isonymy)
        
    departamental_df = pandas.DataFrame(departamental_results)
    departamental_df = departamental_df[DF_COLUMNS]
    
    departamental_df.to_parquet(str(product))

def get_departmental_isonymy_2021(product):
    """"""
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2021.parquet")

    departamental_results = []

    for (departament_id, department_df) in df.groupby('department_id'):
        
        department_isonymy = isonymic.get_isonymic_data(department_df['surname'])
        department_isonymy['division'] = departament_id
        
        departamental_results.append(department_isonymy)
        
    departamental_df = pandas.DataFrame(departamental_results)
    departamental_df = departamental_df[DF_COLUMNS]
    
    departamental_df.to_parquet(str(product))