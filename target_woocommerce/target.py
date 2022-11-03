"""Woocommerce target class."""

from typing import Type

from singer_sdk import typing as th
from singer_sdk.sinks import Sink
from singer_sdk.target_base import Target

from target_woocommerce.sinks import ProductSink, UpdateInventorySink, SalesOrdersSink

SINK_TYPES = [ProductSink, UpdateInventorySink, SalesOrdersSink]


class TargetWoocommerce(Target):
    """Sample target for Woocommerce."""

    name = "target-woocommerce"
    config_jsonschema = th.PropertiesList(
        th.Property("consumer_key", th.StringType, required=True),
        th.Property("consumer_secret", th.StringType, required=True),
        th.Property("site_url", th.StringType, required=True)
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        """Get sink for a stream."""
        return next(
            (
                sink_class
                for sink_class in SINK_TYPES
                if sink_class.name.lower() == stream_name.lower()
            ),
            None,
        )


if __name__ == "__main__":
    TargetWoocommerce.cli()
