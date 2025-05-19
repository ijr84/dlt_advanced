import os
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

#os.environ["RUNTIME__LOG_LEVEL"] = "DEBUG"
os.environ['DATA_WRITER__BUFFER_MAX_ITEMS'] = '2000'
os.environ["EXTRACT__DATA_WRITER__FILE_MAX_ITEMS"] = "5000"
os.environ['EXTRACT__WORKERS'] = '4'
os.environ['NORMALIZE__WORKERS'] = '2'
os.environ["LOAD__WORKERS"] = '2'

@dlt.source
def jaffleshop_source():
    client = RESTClient(
        base_url="https://jaffle-shop.scalevector.ai/api/v1",
        paginator=HeaderLinkPaginator()
    )

    @dlt.resource
    def jaffleshop_customers(table_name="customers", write_disposition="replace", parallelized=True):
        for page in client.paginate("customers?page_size=1000"):
            yield page
    
    @dlt.resource
    def jaffleshop_orders(table_name="orders", write_disposition="replace", parallelized=True):
        for page in client.paginate("orders?page_size=2500"):
            yield page

    @dlt.resource
    def jaffleshop_products(table_name="products", write_disposition="replace", parallelized=True):
        for page in client.paginate("products?page_size=1000"):
            yield page
    
    return jaffleshop_customers, jaffleshop_orders, jaffleshop_products

pipeline = dlt.pipeline(
    pipeline_name="jaffleshop_pipeline",
    destination="duckdb",
    dataset_name="jaffleshop_data",
    dev_mode=True,
)

pipeline.extract(jaffleshop_source())
pipeline.normalize()
pipeline.load()

print(pipeline.last_trace)