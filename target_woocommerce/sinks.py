"""WooCommerce target sink class, which handles writing streams."""

from __future__ import annotations

import html
import json

import requests
from random_user_agent.user_agent import UserAgent
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError
from singer_sdk.sinks import RecordSink

from target_woocommerce.mapper import orders_from_unified, products_from_unified


class WooCommerceSink(RecordSink):
    """WooCommerce target sink class."""

    user_agents = UserAgent(software_engines="blink", software_names="chrome")
    products = []
    product_ids = {}

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        site_url = self.config["site_url"]
        return f"{site_url}/wp-json/wc/v3/"

    @property
    def authenticator(self):
        """Return a new authenticator object."""
        return (self.config.get("consumer_key"), self.config.get("consumer_secret"))

    @property
    def http_headers(self):
        headers = {}
        headers["Content-Type"] = "application/json"
        # headers["User-Agent"] = self.user_agents.get_random_user_agent().strip()
        return headers

    def get_woo_products(self):
        if len(self.product_ids) > 0:
            return self.product_ids
        else:
            n = 1

            params = {"per_page": 100, "order": "asc", "page": n}

            auth = self.authenticator
            url = f"{self.url_base}products"
            resp = True
            products = []
            while resp:
                resp = requests.get(url=url, auth=auth, params=params)
                self.validate_response(resp)
                resp = resp.json()
                n += 1
                params.update({"page": n})
                products += resp

            product_ids = {}

            for product in products:
                product_ids.update({product["sku"]: product["id"]})
            self.product_ids = product_ids
            self.products = products
            return self.product_ids

    def get_product_categories(self) -> dict:

        auth = self.authenticator
        url = f"{self.url_base}products/categories"
        resp = requests.get(url=url, auth=auth)
        self.validate_response(resp)
        resp = resp.json()
        return {html.unescape(i["name"]): i["id"] for i in resp}

    def update_product_categories(self, category_name) -> None:

        if category_name == None:
            return {}
        auth = self.authenticator
        url = f"{self.url_base}products/categories"
        resp = requests.post(url=url, auth=auth, data={"name": category_name})
        self.validate_response(resp)

    def find_product(self, filter_key, filter_val):
        ret_product = {}
        if len(self.products) > 0:
            for product in self.products:
                if filter_key in product:
                    if product[filter_key] == filter_val:
                        ret_product = product
                        break
        return ret_product

    def process_record(self, record: dict, context: dict) -> None:
        product = None
        streams = {
            "Products": "products",
            "SalesOrders": "orders",
            "UpdateInventory": "products",
        }
        method = "POST"
        # Products
        if self.stream_name == "Products":
            record = products_from_unified(record)

            if not context.get("product_categories"):
                context["product_categories"] = self.get_product_categories()

            if not record.get("categories") in context["product_categories"]:
                self.update_product_categories(record.get("categories"))
                context["product_categories"] = self.get_product_categories()
            elif record.get("categories") is not None:
                record["categories"] = [
                    {"id": context["product_categories"][record["categories"]]}
                ]

        # Sales Orders
        if self.stream_name == "SalesOrders":
            record = orders_from_unified(record)

            products = self.get_woo_products()

            record_line_items = record["line_items"]

            record_line_items_ = [] 
            for i in record_line_items: 
                
                if i['product_id'] is not None and int(i["product_id"]) in products.values(): 
                    {"product_id": i["product_id"], "quantity": i["quantity"]}
                elif i['sku'] is not None and i["sku"] in products.keys(): 
                    record_line_items_.append({"product_id": products[i["sku"]], "quantity": i["quantity"]})
                else: 
                    raise Exception(f"PRODUCT ID NOT FOUND FOR ORDER LINE :{i}") 

            record.update({"line_items": record_line_items_})


        # Update Product Inventory
        if self.stream_name == "UpdateInventory":
            method = "PUT"
            ids = self.get_woo_products()
            # find product by id
            if len(record["id"]) > 0:
                product = self.find_product("id", int(record["id"]))
            elif len(record["sku"]) > 0:
                product = self.find_product("sku", record["sku"])
            elif len(record["name"]) > 0:
                product = self.find_product("name", record["name"])

            if product:
                in_stock = True
                current_stock = product.get("stock_quantity", 0)
                if record["operation"] == "subtract":
                    current_stock = current_stock - int(record["quantity"])
                else:
                    current_stock = current_stock + int(record["quantity"])

                if current_stock <= 0:
                    in_stock = False

                product.update(
                    {
                        "stock_quantity": current_stock,
                        "manage_stock": True,
                        "in_stock": in_stock,
                    }
                )
                record = product

        url = f"{self.url_base}{streams[self.stream_name]}"

        headers = self.http_headers
        auth = self.authenticator
        if method == "POST":
            resp = requests.post(
                url=url, headers=headers, auth=auth, data=json.dumps(record)
            )
            self.validate_response(resp)

        if method == "PUT" and product is not None:
            url = f"{url}/{product['id']}"
            resp = requests.put(url=url, headers=headers, auth=auth, json=record)
            self.validate_response(resp)

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response."""
        if (
            response.status_code >= 400
            and self.config.get("ignore_server_errors")
            and self.error_counter < 10
        ):
            self.error_counter += 1
        elif 500 <= response.status_code < 600 or response.status_code in [429]:
            msg = (
                f"{response.status_code} Server Error: "
                f"{response.reason} for path: {self.stream_name}"
                f"{response.text}"
            )
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.stream_name}"
                f"{response.text}"
            )
            raise FatalAPIError(msg)
