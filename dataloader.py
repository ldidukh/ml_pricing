import os
import json
import numpy as np
import pandas as pd
import psycopg2
from ds_base.database.connections import PSQL
from ds_experiments.asin_date_fields import AsinFields

def move_to_json(x):
    """_summary_

    Args:
        x (_type_): _description_

    Returns:
        _type_: _description_
    """
    try:
        res = json.loads(x)
    except Exception as e:
        res = None
    return res

def unnest_categories(categories):
    """_summary_

    Args:
        categories (_type_): _description_

    Returns:
        _type_: _description_
    """
    d = {}
    for i in range(len(categories)):
        d[f'category{i+1}'] = categories[i]['category']
        d[f'rank{i+1}'] = categories[i]['rank']
    return d

class Dataset():
    """

    Returns:
        _type_: _description_
    """
    def __init__(self, config, preprocess) -> None:
        self.config = config
        self.query_path = config.get('query_path')
        self.start_date =  config.get('start_date')
        self.end_date = config.get('end_date')
        self.preprocess = preprocess
        return

    def load_data(self, preprocess):
        """
        Load dataset and process it by input function
        Returns:
            _type_: _description_
        """

        ### Read the information from the argparser:
        queries_path = "../queries/queries.sql"
        psql = PSQL(path_to_queries=queries_path)
        psql_tier_df = psql.query("asin_tier_report_sql")

        start_date="2020-01-01"
        end_date="2022-02-01"
        start_date = self.start_date
        end_date = self.end_date

        asins = list(psql_tier_df.asin.unique())
        asin_field = AsinFields(asins=asins, start_date=start_date,  end_date=end_date)
       
        aggr_revenue = asin_field.get_fields_df(['gross_revenue']).groupby("asin").sum()
        psql_tier_df = aggr_revenue.merge(psql_tier_df, on="asin")
        parent_asins = psql_tier_df[['parent_asin','gross_revenue']].groupby('parent_asin').sum().index
        top_asin = psql_tier_df.sort_values(['parent_asin','gross_revenue']).dropna(subset=['parent_asin']).drop_duplicates('parent_asin',keep='last')

        asins_list = top_asin.sort_values('gross_revenue', ascending=False).asin.to_list()
        asin_field = AsinFields(asins=asins_list, start_date=start_date,end_date=end_date, deduplicate_dates=True)
        fields = ['rating','ratings_total','sp_spend']
        groups = ['amz_sales']

        asin_fields_df_path = os.path.join('./', f'asin_fields_2years_300asins_.parquet')
        if not os.path.isfile(asin_fields_df_path):

            asin_sales_fields_df = asin_100_field.get_field_groups_df(groups)
            asin_fields_df = asin_100_field.get_fields_df(fields)
            asin_fields_df = asin_fields_df.merge(asin_sales_fields_df, on=['asin', 'date'], how='outer')
            asin_fields_df.to_parquet(asin_fields_df_path)

        else:
            asin_fields_df = pd.read_parquet(asin_fields_df_path)
            
        ##Load best-seller rank:
        osr_params = {'start_date': start_date,'end_date': end_date,'asins': tuple(asins_100_list)}
        psql_bsr_df = psql.query("rf1_pdp_asins_bsr_ratings_total",params=osr_params)

        psql_bsr_df['bsr'] = psql_bsr_df['bestsellers_rank'].apply(lambda x: move_to_json(x))
        psql_bsr_df['num_categories'] = psql_bsr_df['bsr'].apply(lambda x: len(x) if x is not None else 0)
        psql_bsr_df = psql_bsr_df.drop_duplicates(['date', 'asin']).reset_index(drop=True)

        categories_df = pd.DataFrame(psql_bsr_df.set_index(['asin', 'date'], drop=False)['bsr'].apply( lambda x: unnest_categories(x) if x is not None else {}).tolist())

        categories_df['min_rank'] = categories_df[[f'rank{i}' for i in range(1,4)]].min(axis=1)
        categories_df['bsb'] = categories_df['min_rank'] .apply(lambda x: 1 if x==1 else 0)
        categories_df['date'] = psql_bsr_df['date']
        categories_df['asin'] = psql_bsr_df['asin']

        categories_df['min_rank'] = categories_df[[f'rank{i}' for i in range(1,4)]].min(axis=1)
        categories_df['bsb'] = categories_df['min_rank'] .apply(lambda x: 1 if x==1 else 0)
        categories_df['date'] = psql_bsr_df['date']
        categories_df['asin'] = psql_bsr_df['asin']

        categories_df = categories_df.set_index(['asin', 'date'], drop=False)
        asin_fields_df = asin_fields_df.set_index(['asin', 'date'],drop=False)
        categories_df = categories_df.reset_index(drop=True)
        asin_fields_df = asin_fields_df.reset_index(drop=True)
        categories_df['date'] = pd.to_datetime(categories_df['date'])
        df = pd.merge(asin_fields_df, categories_df,on=['asin', 'date'],  how='right')
        self.panel_df = df
        
    def __len__(self):
        """_summary_

        Returns:
            _type_: _description_
        """
        return self.list_of_asin
    
    def get_item(self, indx):
        """_summary_

        Returns:
            _type_: _description_
        """
        ts = self.panel_df.loc[indx]
        return ts

