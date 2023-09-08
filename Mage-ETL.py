___________________________________________________PRIYANTHAN _____________________________________________________
########################################------------ DATA EXTRACT -----------------------------#######################
import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://storage.googleapis.com/cargils_assignment_bucket/sales_data.csv'
    response = requests.get(url)

    return pd.read_csv(io.StringIO(response.text), sep=',')


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

########################################------------ DATA TRANSFORM -----------------------------#######################


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    df['ORDERDATE'] = pd.to_datetime(df['ORDERDATE'])
    df = df.drop_duplicates().reset_index(drop =True)
    df['SALES_ID'] = df.index

    # time_dim dimension table
    time_dim = df[['QTR_ID','ORDERDATE','MONTH_ID','YEAR_ID']].reset_index(drop=True)
    time_dim['ORDERDATE'] = time_dim['ORDERDATE']
    time_dim['DAY_ID'] = time_dim['ORDERDATE'].dt.day
    time_dim['WEEKDAY'] = time_dim['ORDERDATE'].dt.weekday
    time_dim['TIME_ID'] = time_dim.index
    time_dim = time_dim[['TIME_ID','ORDERDATE','DAY_ID','MONTH_ID','YEAR_ID' , 'WEEKDAY' , 'QTR_ID']]

    #dealsize_dim dimension table
    dealsize_dim = df[['DEALSIZE']].reset_index(drop = True)
    dealsize_dim['DEAL_ID'] = dealsize_dim.index
    dealsize_dim = dealsize_dim[['DEAL_ID','DEALSIZE']]

    # product_dim dimension table
    product_dim = df[['PRODUCTCODE','PRODUCTLINE','MSRP']].reset_index(drop = True)
    product_dim['PRODUCT_ID'] = product_dim.index
    product_dim = product_dim[['PRODUCT_ID', 'PRODUCTLINE' , 'PRODUCTCODE' , 'MSRP']]

    # customer_dim dimension table
    customer_dim = df[['CUSTOMERNAME','CONTACTFIRSTNAME','CONTACTLASTNAME','PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2','POSTALCODE','CITY','STATE','COUNTRY','TERRITORY']].reset_index(drop =True)
    customer_dim['CUSTOMER_ID'] = customer_dim.index
    customer_dim = customer_dim[['CUSTOMER_ID','CUSTOMERNAME','CONTACTFIRSTNAME','CONTACTLASTNAME','PHONE', 'ADDRESSLINE1', 'ADDRESSLINE2','POSTALCODE','CITY','STATE','COUNTRY','TERRITORY']]

    Salesfact_table = df.merge(time_dim, left_on='SALES_ID', right_on='TIME_ID') \
             .merge(dealsize_dim, left_on='SALES_ID', right_on='DEAL_ID') \
             .merge(product_dim, left_on='SALES_ID', right_on='PRODUCT_ID') \
             .merge(customer_dim, left_on='SALES_ID', right_on='CUSTOMER_ID') \
             [['SALES_ID','TIME_ID', 'DEAL_ID', 'PRODUCT_ID',
               'CUSTOMER_ID', 'ORDERNUMBER', 'QUANTITYORDERED','PRICEEACH','SALES', 'STATUS',
                'ORDERLINENUMBER']]

    return {"time_dim":time_dim.to_dict(orient="dict"),
    "dealsize_dim":dealsize_dim.to_dict(orient="dict"),
    "product_dim":product_dim.to_dict(orient="dict"),
    "customer_dim":customer_dim.to_dict(orient="dict"),
    "Salesfact_table":Salesfact_table.to_dict(orient="dict")}

    return Salesfact_table


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'


########################################------------ DATA LOAD(big query) -----------------------------#######################


from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_big_query(data: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery
    """
    table_id = 'cargils-assignment-pri.cargils_assignment_ds.Salesfact_table'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        DataFrame(data["Salesfact_table"]),
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )
    table_id = 'cargils-assignment-pri.cargils_assignment_ds.time_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        DataFrame(data["time_dim"]),
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )
    table_id = 'cargils-assignment-pri.cargils_assignment_ds.dealsize_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        DataFrame(data["dealsize_dim"]),
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )
    table_id = 'cargils-assignment-pri.cargils_assignment_ds.product_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        DataFrame(data["product_dim"]),
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )
    table_id = 'cargils-assignment-pri.cargils_assignment_ds.customer_dim'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
        DataFrame(data["customer_dim"]),
        table_id,
        if_exists='replace',  # Specify resolution policy if table name already exists
    )

