"""Woocommerce target sink class, which handles writing streams."""
import re

from hotglue_models_ecommerce.ecommerce import SalesOrder, Product, OrderNote
from target_woocommerce.local_models import UpdateInventory

from target_woocommerce.client import WoocommerceSink
from backports.cached_property import cached_property

class SalesOrdersSink(WoocommerceSink):
    """Woocommerce order target sink class."""

    endpoint = "orders"
    unified_schema = SalesOrder
    name = SalesOrder.Stream.name

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            record = self.validate_input(record)
            if record.get("customer_name"):
                customer_name = record.get("customer_name").split(" ")
                first_name = customer_name[0]
                last_name = " ".join(customer_name[1:])
            else:
                first_name = ""
                last_name = ""
            mapping = {}
            billing_address = record.get("billing_address", {})
            shipping_address = record.get("shipping_address", {})
            order_id = record.get("id") or record.get("order_number")
            if isinstance(order_id, str):
                order_id = order_id.replace("#", "")
                order_id = int(order_id)

            mapping["id"] = order_id
            mapping["status"] = record.get("status")

            if billing_address:
                mapping["billing"] = {
                    "first_name": first_name,
                    "last_name": last_name,
                    "address_1": billing_address.get("line1"),
                    "address_2": billing_address.get("line2"),
                    "city": billing_address.get("city"),
                    "state": billing_address.get("state"),
                    "postcode": billing_address.get("postal_code"),
                    "country": billing_address.get("country"),
                    "email": billing_address.get("customer_email"),
                }
            if shipping_address:
                mapping["shipping"] = {
                    "first_name": first_name,
                    "last_name": last_name,
                    "address_1": shipping_address.get("line1"),
                    "address_2": shipping_address.get("line2"),
                    "city": shipping_address.get("city"),
                    "state": shipping_address.get("state"),
                    "postcode": shipping_address.get("postal_code"),
                    "country": shipping_address.get("country"),
                }
                if shipping_address.get("total_shipping"):
                    mapping["shipping_lines"] = [
                        {"total": shipping_address["total_shipping"]}
                    ]
            status = record.get("status")
            fulfilled = record.get("fulfilled")
            if fulfilled:
                mapping["status"] = "completed"
            if fulfilled is False:
                mapping["status"] = status
            if status:
                mapping["status"] = status
            if status == "completed":
                mapping["set_paid"] = True
            else:
                mapping["set_paid"] = record.get("paid", False)
            if record.get("customer_id"):
                mapping["customer_id"] = mapping["customer_id"]
            elif record.get("customer_email"):
                customer = self.get_reference_data(
                    "customers", filter={"email": record["customer_email"]}
                )
                id = next((c["id"] for c in customer), None)
                mapping["customer_id"] = id

            if record["line_items"]:
                if "line_items" not in mapping:
                    mapping["line_items"] = []
                for line in record["line_items"]:
                    if line.get("product_id"):
                        item = {
                            "product_id": line["product_id"],
                            "quantity": line["quantity"],
                        }
                    elif line.get("sku"):
                        product = self.get_reference_data(
                            "products", filter={"sku": line["sku"]}
                        )
                        id = next(p["id"] for p in product)
                        item = {"product_id": id, "quantity": line["quantity"]}
                    else:
                        self.logger.warning(f"Product not found for line item: {line}")
                        # Skip this line item instead of raising an exception
                        continue
                    mapping["line_items"].append(item)

            return self.validate_output(mapping)
        except Exception as e:
            self.logger.error(f"Failed to preprocess {self.name} record: {str(e)}")
            self.logger.error(f"Record data: {record}")
            # Re-raise the exception to be caught by the calling method
            raise

    def report_error_to_job(self, error_message: str, record_data: dict = None):
        """Report error to job output for better visibility."""
        # Log error with high visibility
        self.logger.error(f"JOB ERROR: {error_message}")
        if record_data:
            self.logger.error(f"JOB ERROR - Record data: {record_data}")
        
        # Also log as critical to ensure it appears in job output
        self.logger.critical(f"CRITICAL ERROR: {error_message}")
        
        # Print to stderr for immediate visibility
        import sys
        print(f"ERROR: {error_message}", file=sys.stderr)
        if record_data:
            print(f"ERROR - Record data: {record_data}", file=sys.stderr)

    def process_record(self, record: dict, context: dict) -> None:
        """Override Hotglue SDK's process_record to ensure preprocess_record is called first."""
        # Track total records
        self.export_stats["total_records"] += 1
        
        try:
            # First, preprocess the record
            preprocessed_record = self.preprocess_record(record, context)
            
            # If preprocessing failed (returned None or skip marker), skip this record
            if preprocessed_record is None or preprocessed_record.get("_skip"):
                if preprocessed_record and preprocessed_record.get("_skip"):
                    error_msg = preprocessed_record.get("error", f"Skipping {self.name} record as preprocessing failed")
                else:
                    error_msg = f"Skipping {self.name} record as preprocessing failed"
                # Don't call report_failure again as it was already called in preprocess_record
                return None, False, {"error": error_msg}
            
            # Then, upsert the preprocessed record
            result = self.upsert_record(preprocessed_record, context)
            
            # Report success if the operation was successful
            if result and result[1]:  # result[1] is the success flag
                record_id = result[0] if result[0] else "unknown"
                self.report_success(record_id, "updated" if result[2].get("updated") else "created")
            
            return result
            
        except Exception as e:
            return self._handle_operation_error("process", e, record)

    def upsert_record(self, record: dict, context: dict) -> None:
        try:
            if "id" in record:
                endpoint = f"orders/{record['id']}"
                response = self.request_api(
                    "PUT", endpoint=endpoint, request_data=record
                )
                order_response = response.json()
                id = order_response.get("id")
                self._log_operation_success("upsert", str(id), "updated")
                
                return id, response.ok, {"updated": True}
            else:
                response = self.request_api("POST", request_data=record)
                id = response.json().get("id")
                self._log_operation_success("upsert", str(id), "created")
                
                return id, response.ok, dict()
        except Exception as e:
            return self._handle_operation_error("upsert", e, record)



class UpdateInventorySink(WoocommerceSink):
    """Woocommerce inventory target sink class."""

    endpoint = "products/{id}"
    unified_schema = UpdateInventory
    name = UpdateInventory.Stream.name

    @cached_property
    def products(self):
        endpoint = "products"
        fields = ["id", "name", "sku", "stock_quantity", "type"]
        return self.get_reference_data(endpoint, fields)

    @cached_property
    def product_variants(self):
        variants = []
        products = self.products
        fields = ["id", "name", "sku", "stock_quantity"]

        for product in products:
            if product["type"] != "variable":
                continue

            parent_id = product["id"]
            data = self.get_reference_data(
                f"products/{parent_id}/variations", fields, fallback_url="products/"
            )
            for d in data:
                # save the parent_id so we know it is a variant
                d["parent_id"] = parent_id
            variants += data

        return variants

    def _get_alnum_string(self, input):
        return re.sub(r"\W+", "", input)

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            # Check if this record is already preprocessed
            if record.get("_preprocessed"):
                self.logger.info(f"Record already preprocessed, returning as-is")
                return record
            
            # Validate the input record first (this validates against UpdateInventory schema)
            validated_record = self.validate_input(record)
            
            # Handle product_name mapping
            if "product_name" in validated_record.keys():
                validated_record["name"] = validated_record["product_name"]

            product = None
            product_id = validated_record.get("id")
            product_sku = validated_record.get("sku")
            product_name = validated_record.get("name")
            
            # Two-way approach: Use ID directly when available, fall back to SKU lookup
            if product_id:
                # If we have an ID, try to use it directly first
                self.logger.info(f"Using provided ID {product_id} directly for inventory update")
                
                # Fetch the actual product data from API to get current stock
                try:
                    endpoint = f"products/{product_id}"
                    response = self.request_api("GET", endpoint)
                    product_data = response.json()
                    
                    # Create product object with actual data from API
                    product = {
                        "id": int(product_id),
                        "name": product_data.get("name", product_name or f"Product {product_id}"),
                        "sku": product_data.get("sku", product_sku),
                        "stock_quantity": product_data.get("stock_quantity", 0),
                        "type": product_data.get("type", "simple")
                    }
                    
                    # For variations, we need to get the parent_id
                    if product_data.get("type") == "variation":
                        # The product_data should already contain parent_id for variations
                        if product_data.get("parent_id"):
                            product["parent_id"] = product_data["parent_id"]
                            self.logger.info(f"Variation {product_id} belongs to parent product {product_data['parent_id']}")
                        else:
                            self.logger.warning(f"Variation {product_id} has no parent_id, falling back to reference data lookup")
                            # If we can't get parent_id, we'll fall back to reference data lookup
                            product = None
                    self.logger.info(f"Fetched current stock for ID {product_id}: {product['stock_quantity']}")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to fetch product data for ID {product_id}, falling back to reference data lookup: {str(e)}")
                    # Fall back to reference data lookup if API call fails
                    product = None
                
            # Fall back to SKU lookup only if we don't have an ID or ID lookup failed
            if not product and product_sku:
                self.logger.info(f"No ID provided, attempting SKU lookup for {product_sku}")
                
                # Check main product sku
                product_list = [p for p in self.products if p.get("sku")==product_sku]
                if len(product_list) > 1:
                    self.logger.info(f"More than one product was found with sku {product_sku}, filtering product by name...")
                    product = next((p for p in product_list if p.get("name") == product_name), None)
                elif len(product_list) == 1:
                    product = product_list[0]
                
                # If not found in main products, check variants
                if not product:
                    self.logger.info(f"Attempting to match product with sku {product_sku} in variants...")
                    product_list = [p for p in self.product_variants if p.get("sku")==product_sku]
                    if len(product_list) > 1:
                        self.logger.info(f"More than one product was found with sku {product_sku}, filtering product by name...")
                        product = next((p for p in product_list if p.get("name") == product_name), None)
                    elif len(product_list) == 1:
                        product = product_list[0]
            
            # Last resort: try name matching
            elif product_name:
                self.logger.info(f"No ID or SKU provided, attempting name lookup for {product_name}")
                product = next((p for p in self.products if p.get("name") == product_name), None)
                
                if not product:
                    self.logger.info(f"Attempting to match product with name {product_name} in variants...")
                    product = next(
                        (p for p in self.product_variants if p.get("name") == product_name), None
                    )
                
                if not product:
                    self.logger.info(f"Attempting to match product with sanitized name in variants...")
                    # Some items may vary on naming and special characters
                    product = next(
                        (
                            p
                            for p in self.product_variants
                            if self._get_alnum_string(p.get("name"))
                            == self._get_alnum_string(product_name)
                        ),
                        None,
                    )

            if product:
                self.logger.info(f"Found product: {product}")
                _product = product.copy()
                in_stock = True
                current_stock = _product.get("stock_quantity") or 0
                self.logger.info(f"product with sku '{_product.get('sku')}' and id {_product.get('id', 'unknown')} current stock: {current_stock}, executing operation '{validated_record['operation']}' with quantity {validated_record['quantity']}")

                if validated_record["operation"] == "subtract":
                    current_stock = current_stock - int(validated_record["quantity"])
                if validated_record["operation"] == "set":
                    current_stock = int(validated_record["quantity"])
                else:
                    current_stock = current_stock + int(validated_record["quantity"])

                if current_stock <= 0:
                    in_stock = False

                _product.update(
                    {
                        "stock_quantity": current_stock,
                        "manage_stock": True,
                        "in_stock": in_stock,
                    }
                )
                # remove sku from payload to avoid duplicate sku issues
                _product.pop("sku", None)
                
                # Return the preprocessed product data directly (don't validate against UpdateInventory schema)
                # Ensure the returned data is clean and won't cause validation issues
                cleaned_product = self.clean_payload(_product)
                
                # Add a flag to indicate this is preprocessed data to prevent double processing
                cleaned_product["_preprocessed"] = True
                
                return cleaned_product
            else:
                # Build a detailed error message showing what was attempted
                attempted_lookups = []
                if validated_record.get("id"):
                    attempted_lookups.append(f"ID '{validated_record['id']}'")
                if validated_record.get("sku"):
                    attempted_lookups.append(f"SKU '{validated_record['sku']}'")
                if validated_record.get("name"):
                    attempted_lookups.append(f"name '{validated_record['name']}'")
                
                error_msg = f"Product not found: Attempted lookup by {', '.join(attempted_lookups)} but no matching product exists in WooCommerce. Skipping record."
                self.report_failure(error_msg, validated_record)
                # Return a special marker to indicate this record should be skipped
                return {"_skip": True, "error": error_msg}

        except Exception as e:
            error_msg = f"Failed to preprocess {self.name} record: {str(e)}"
            self.report_failure(error_msg, record)
            # Re-raise the exception to be caught by the calling method
            raise



    def upsert_record(self, record: dict, context: dict) -> None:
        """Upsert the record."""
        try:
            # Check if this is a skip marker
            if record.get("_skip"):
                error_msg = record.get("error", "Record skipped")
                return None, False, {"error": error_msg, "skipped": True}
            
            # Ensure we have the required 'id' field
            if "id" not in record:
                raise KeyError("id")
            
            if record.get("parent_id"):
                endpoint = (
                    self.endpoint.format(id=record["parent_id"])
                    + "/variations/"
                    + str(record["id"])
                )
            else:
                endpoint = self.endpoint.format(id=record["id"])

            response = self.request_api("PUT", endpoint, request_data=record)
            product_response = response.json()
            id = product_response.get("id")
            self._log_operation_success("upsert", str(id), "updated")
            
            return id, response.ok, {"updated": True}
        except Exception as e:
            return self._handle_operation_error("upsert", e, record)

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

    def get_existing_id(self, variant):
        try:
            if variant.get("id"):
                resp = self.request_api("GET", "products", {"include": [variant["id"]]})
                resp = resp.json()
                if resp:
                    return {k: v for k, v in resp[0].items() if k in ["id", "type"]}
            if variant.get("sku"):
                resp = self.request_api("GET", "products", {"sku": variant["sku"]})
                resp = resp.json()
                if resp:
                    return {
                        k: v for k, v in resp[0].items() if k in ["id", "type", "parent_id"]
                    }
            return None
        except Exception as e:
            self.logger.error(f"Failed to get existing ID for variant: {str(e)}")
            self.logger.error(f"Variant data: {variant}")
            return None

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            record = self.validate_input(record)

            for variant in record.get("variants") or []:
                product_id = self.get_existing_id(variant)
                if product_id:
                    variant["id"] = product_id["id"]
                    if product_id.get("parent_id"):
                        record["id"] = product_id["parent_id"]
                    record["type"] = "variable" if product_id["parent_id"] else "simple"

            if not record.get("type"):
                record["type"] = "variable" if record.get("options") else "simple"

            mapping = {
                "name": record["name"],
                "sku": record.get("sku"),
                "description": record.get("description"),
                "short_description": record.get("short_description"),
                "type": record["type"],
            }

            if record.get("id"):
                mapping["id"] = record["id"]

            if record.get("image_urls"):
                mapping["images"] = [{"src": i} for i in record["image_urls"]]

            if record.get("category"):
                ctg = record["category"]
                if ctg.get("id"):
                    mapping["categories"] = [{"id": ctg["id"]}]
                else:
                    ctg = [
                        {"id": c["id"]} for c in self.categories if c["name"] == ctg["name"]
                    ]
                    mapping["categories"] = ctg
            elif record.get("categories"):
                categories = record["categories"]
                mapping["categories"] = []

                for ctg in categories:
                    if ctg.get("id"):
                        mapping["categories"].append({"id": ctg["id"]})
                    else:
                        ctg = [
                            {"id": c["id"]}
                            for c in self.categories
                            if c["name"] == ctg["name"]
                        ]
                        mapping["categories"] += ctg

            if record["type"] == "variable":
                mapping["variations"] = []
                for variant in record["variants"]:
                    product_var = {
                        "sku": variant.get("sku"),
                        "regular_price": str(variant.get("price")),
                        "sale_price": str(variant.get("sale_price")),
                        "manage_stock": True,
                        "stock_quantity": variant.get("available_quantity"),
                        "weight": variant.get("weight"),
                        "description": variant.get("description"),
                        "dimensions": {
                            "width": variant.get("width"),
                            "length": variant.get("length"),
                            "height": variant.get("depth"),
                        },
                    }

                    if variant.get("id"):
                        product_var["id"] = variant["id"]

                    product_var["attributes"] = []

                    if variant.get("options"):
                        for option in variant["options"]:
                            product_var["attributes"].append(
                                dict(name=option["name"], option=option["value"])
                            )
                    mapping["variations"].append(product_var)
                # Process attributes
                if variant.get("options"):
                    variant_options = []
                    for variant in record["variants"]:
                        variant_options += variant["options"]
                    attributes = []
                    default_attributes = []
                    for option in record["options"]:
                        options = [
                            v["value"] for v in variant_options if v["name"] == option
                        ]
                        if not options:
                            continue
                        default_attribute = dict(option=options[0])
                        attribute = {
                            "position": 0,
                            "visible": False,
                            "variation": True,
                            "options": options,
                        }
                        id = next(
                            (a["id"] for a in self.attributes if a["name"] == option), None
                        )
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
                if record.get("variants") and len(record["variants"]) > 0:
                    variant = record["variants"][0]
                    product_id = self.get_existing_id(variant)

                    mapping.update(
                        {
                            "sku": variant.get("sku"),
                            "regular_price": str(variant.get("price")),
                            "manage_stock": True,
                            "stock_quantity": variant.get("available_quantity"),
                            "weight": variant.get("weight"),
                            "dimensions": {
                                "width": variant.get("width"),
                                "length": variant.get("length"),
                                "height": variant.get("depth"),
                            },
                        }
                    )
                    if product_id:
                        mapping["id"] = product_id["id"]

            return self.validate_output(mapping)
        except Exception as e:
            self.logger.error(f"Failed to preprocess {self.name} record: {str(e)}")
            self.logger.error(f"Record data: {record}")
            # Re-raise the exception to be caught by the calling method
            raise

    def process_variation(self, record: dict, prod_response) -> None:
        """Process the record."""
        try:
            product_id = prod_response["id"]
            url = f"products/{product_id}/variations"
            for variation in record["variations"]:
                try:
                    if "id" in variation:
                        endpoint = f"{url}/{variation['id']}"
                        response = self.request_api(
                            "PUT", endpoint=endpoint, request_data=variation
                        )
                        product_response = response.json()
                        id = product_response.get("id")
                        self.logger.info(f"Variation {id} updated.")
                    else:
                        for attr in variation["attributes"]:
                            sel_attr = next(
                                a
                                for a in prod_response["attributes"]
                                if a["name"] == attr["name"]
                            )
                            attr["id"] = sel_attr["id"]
                        response = self.request_api(
                            "POST", endpoint=url, request_data=variation
                        )
                        variant_response = response.json()
                        self.logger.info(f"Created variant with id: {variant_response['id']}")
                except Exception as e:
                    self.logger.error(f"Failed to process variation: {str(e)}")
                    self.logger.error(f"Variation data: {variation}")
                    # Continue with next variation instead of stopping
                    continue
        except Exception as e:
            self.logger.error(f"Failed to process variations for product: {str(e)}")
            # Don't re-raise the exception to allow the main upsert to continue

    def upsert_record(self, record: dict, context: dict) -> None:
        try:
            if "id" in record:
                endpoint = f"products/{record['id']}"
                response = self.request_api("PUT", endpoint=endpoint, request_data=record)
                product_response = response.json()
                id = product_response.get("id")
                self.logger.info(f"{self.name} {id} updated.")
                
                # Report success
                if id:
                    self.report_success(str(id), "updated")
                
                if record["type"] == "variable":
                    self.process_variation(record, product_response)
                return id, response.ok, {"updated": True}
            else:
                response = self.request_api("POST", request_data=record)
                product_response = response.json()
                id = product_response.get("id")
                self.logger.info(f"{self.name} created with id: {id}")
                
                # Report success
                if id:
                    self.report_success(str(id), "created")
                
                if record["type"] == "variable":
                    self.process_variation(record, product_response)
                return id, response.ok, dict()
        except Exception as e:
            return self._handle_operation_error("upsert", e, record)

        
class OrderNotesSink(WoocommerceSink):
    """Woocommerce order target sink class."""

    endpoint = "orders/{order_id}/notes"
    unified_schema = OrderNote
    name = OrderNote.Stream.name
    available_names = [OrderNote.Stream.name, "OrderNote"]

    def preprocess_record(self, record: dict, context: dict) -> dict:
        try:
            record = self.validate_input(record)
            #Going to skip id because could not find PUT/Update endpoint for Notes
            mapping = {
              "order_id": record.get("order_id"),
              "author": record.get("author_name"),
              "note": record.get("note"),
              "date_created": record.get("created_at"),
            }
            if "customer_note" in record:
                try:
                    mapping['customer_note'] = record.get("customer_note")
                except:
                    mapping['customer_note'] = False


            return self.validate_output(mapping)
        except Exception as e:
            self.logger.error(f"Failed to preprocess {self.name} record: {str(e)}")
            self.logger.error(f"Record data: {record}")
            # Re-raise the exception to be caught by the calling method
            raise
    
    def upsert_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        try:
            if "order_id" in record:
                endpoint = f"orders/{record['order_id']}/notes"
                response = self.request_api("POST", endpoint=endpoint, request_data=record)
                product_response = response.json()
                id = product_response.get("id")
                self.logger.info(f"{self.name} {id} added.")
                
                # Report success
                if id:
                    self.report_success(str(id), "created")
                
                return id, response.ok, dict()
            else:
                self.logger.warn(f"{self.name} had no order_id skipped note {record.get('note')}")
                return None, False, {"error": "No order_id provided"}
        except Exception as e:
            return self._handle_operation_error("upsert", e, record)