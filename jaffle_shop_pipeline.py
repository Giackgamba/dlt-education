import os

import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import HeaderLinkPaginator

client = RESTClient(
    base_url="https://jaffle-shop.scalevector.ai/api/v1",
    paginator=HeaderLinkPaginator(),
)


@dlt.resource(name="customers")
def customer_resource(page_size: int = 100):
    for page in client.paginate("/customers", params={"page_size": page_size}):
        yield page


@dlt.resource(name="products")
def products_resource(page_size: int = 100):
    for page in client.paginate("/products", params={"page_size": page_size}):
        yield page


@dlt.resource(name="orders")
def orders_resource(page_size: int = 100):
    for page in client.paginate("/orders", params={"page_size": page_size}):
        yield page


@dlt.source
def jaffle_shop_data(page_size: int = 100):
    return [customer_resource(page_size), products_resource(page_size), orders_resource(page_size)]


os.environ["EXTRACT__WORKERS"] = "50"
os.environ["EXTRACT__FILE_MAX_BYTES"] = "1000000"
os.environ["NORMALIZE__WORKERS"] = "20"
os.environ["LOAD__WORKERS"] = "20"

if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="jaffle_shop_pipeline",
        destination="duckdb",
        dataset_name="jaffle_shop_data",
    )
    load_info = pipeline.run(jaffle_shop_data(10).parallelize().add_limit(5))
    print(pipeline.last_trace)
    print(load_info)
