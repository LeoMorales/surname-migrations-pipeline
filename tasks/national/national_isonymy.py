import pandas
from surnames_package import isonymic

DF_COLUMNS = ['division', 'n', 'ins', 'fst', 'fishers_alpha', 'A', 'B' ]

def get_national_isonymy_2015(product):
    """"""
    df = pandas.read_parquet("/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet")
    
    isonymic_result = isonymic.get_isonymic_data(df['surname'])
    data = [["Argentina"] + list(isonymic_result.values())]
    columns = ["division"] + list(isonymic_result.keys())
    
    output_df = pandas.DataFrame(data=data, columns=columns)
    output_df.to_parquet(str(product))