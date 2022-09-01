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

    def get_product_categories(self) -> dict:

        auth = self.authenticator
        url = f"{self.url_base}products/categories"
        resp = requests.get(url=url, auth=auth)
        self.validate_response(resp)
        resp = resp.json()
        return {html.unescape(i["name"]): i["id"] for i in resp}

    def update_product_categories(self, category_name) -> None:

        auth = self.authenticator
        url = f"{self.url_base}products/categories"
        resp = requests.post(url=url, auth=auth, data={"name": category_name})
        self.validate_response(resp)

    def process_record(self, record: dict, context: dict) -> None:
        streams = {"Products": "products", "Sales Orders": "orders"}

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
        if self.stream_name == "Sales Orders":
            record = orders_from_unified(record)

        url = f"{self.url_base}{streams[self.stream_name]}"

        headers = self.http_headers
        auth = self.authenticator

        resp = requests.post(
            url=url, headers=headers, auth=auth, json=json.dumps(record)
        )
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
                f"{response.reason} for path: {self.path}"
            )
            raise RetriableAPIError(msg)
        elif 400 <= response.status_code < 500:
            msg = (
                f"{response.status_code} Client Error: "
                f"{response.reason} for path: {self.path}"
            )
            raise FatalAPIError(msg)
