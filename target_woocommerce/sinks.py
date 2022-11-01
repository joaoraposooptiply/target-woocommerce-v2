"""Woocommerce target sink class, which handles writing streams."""
from hotglue_models_ecommerce.ecommerce import SalesOrder, Product
from target_woocommerce.local_models import UpdateInventory

from target_woocommerce.client import WoocommerceSink
from backports.cached_property import cached_property


# class SalesOrdersSink(WoocommerceSink):
#     """Woocommerce order target sink class."""

#     endpoint = "/orders/create"
#     unified_schema = SalesOrder
#     name = SalesOrder.Stream.name

#     def preprocess_record(self, record: dict, context: dict) -> dict:
#         record = self.validate_input(record)
        
#         return self.validate_output(mapping)

class UpdateInventorySink(WoocommerceSink):
    """Woocommerce order target sink class."""

    endpoint = "products/{id}"
    unified_schema = UpdateInventory
    name = UpdateInventory.Stream.name

    @cached_property
    def products(self):
        endpoint = "products"
        fields = ["id", "name", "sku", "stock_quantity"]
        return self.get_reference_data(endpoint, fields)

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = self.validate_input(record)

        product_id = record.get("id")
        if product_id:
            product = next((p for p in self.products if p["id"]==product_id), None)
        elif record.get("sku"):
            sku = record.get("sku")
            product = next((p for p in self.products if p["sku"]==sku), None)
        elif record.get("name"):
            name = record.get("name")
            product = next((p for p in self.products if p["name"]==name), None)

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
        else:
            raise Exception("Could not find product with through id, sku or name.")

        return self.validate_output(product)
    
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        endpoint = self.endpoint.format(id=record["id"])
        response = self.request_api("PUT", endpoint, request_data=record)
        product_response = response.json()
        id = product_response.get("id")
        self.logger.info(f"{self.name} updated for id: {id}")


class ProductSink(WoocommerceSink):
    """Woocommerce order target sink class."""

    endpoint = "products"
    unified_schema = Product
    name = Product.Stream.name

    @cached_property
    def categories(self):
        endpoint = "products/categories"
        fields = ["id", "name", "slug"]
        return self.get_reference_data(endpoint, fields)

    @cached_property
    def attributes(self):
        endpoint = "products/attributes"
        fields = ["id", "name", "slug"]
        return self.get_reference_data(endpoint, fields)

    def preprocess_record(self, record: dict, context: dict) -> dict:
        record = self.validate_input(record)

        prd_type = "variable" if record.get("options") else "simple"
        mapping = {
            "name": record["name"],
            "description": record.get("description"),
            "short_description": record.get("short_description"),
            "type": prd_type,
        }
        if record.get("image_urls"):
            mapping["images"] = [{"src": i} for i in record["image_urls"]]

        if record.get("category"):
            ctg = record["category"]
            if ctg.get("id"):
                mapping["categories"] = [{"id": ctg["id"]}]
            else:
                ctg = [{"id": c["id"]} for c in self.categories if c["name"]==ctg["name"]]
                mapping["categories"] = ctg

        if prd_type == "variable":
            mapping["variations"] = []
            for variant in record["variants"]:
                product_var = {
                    "sku": variant.get("sku"),
                    "regular_price": str(variant.get("price")),
                    "manage_stock": True,
                    "stock_quantity": variant.get("available_quantity")
                }
                product_var["attributes"] = []
                for option in variant["options"]:
                    product_var["attributes"].append(dict(name=option["name"], option=option["value"]))
                mapping["variations"].append(product_var)
            # Process attributes
            variant_options = []
            for variant in record["variants"]:
                variant_options += variant["options"]
            attributes = []
            default_attributes = []
            for option in record["options"]:
                options = [v["value"] for v in variant_options if v["name"]==option]
                if not options:
                    continue
                default_attribute = dict(option=options[0])
                attribute = {
                    "position": 0,
                    "visible": False,
                    "variation": True,
                    "options": options
                }
                id = next((a["id"] for a in self.attributes if a["name"]==option), None)
                if id:
                    attribute["id"] = id
                    default_attribute["id"] = id
                else:
                    attribute["name"] = option
                    default_attribute["name"] = option
                attributes.append(attribute)
                default_attributes.append(default_attribute)

                mapping["attributes"] = attributes
                mapping["default_attributes"] = default_attributes
        else:
            variant = record["variants"][0]
            mapping.update({
                    "sku": variant.get("sku"),
                    "regular_price": str(variant.get("price")),
                    "manage_stock": True,
                    "stock_quantity": variant.get("available_quantity")
                })

        return self.validate_output(mapping)
    
    def process_variation(self, record: dict, prod_response) -> None:
        """Process the record."""
        product_id = prod_response["id"]
        url = f"products/{product_id}/variations"
        for variation in record["variations"]:
            for attr in variation["attributes"]:
                sel_attr = next(a for a in prod_response["attributes"] if a["name"]==attr["name"])
                attr["id"] = sel_attr["id"]
            response = self.request_api("POST", endpoint=url, request_data=variation)
            variant_response = response.json()
            self.logger.info(f"Created variant with id: {variant_response['id']}")
    
    def process_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        response = self.request_api("POST", request_data=record)
        product_response = response.json()
        id = product_response.get("id")
        self.logger.info(f"{self.name} created with id: {id}")
        if record["type"] == "variable":
            self.process_variation(record, product_response)
        
        