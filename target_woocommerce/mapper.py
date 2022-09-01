def products_from_unified(record):

    mapp = {
        "name": "name",
        "variant": "type",
        "price": "regular_price",
        "description": "description",
        "short_description": "short_description",
        "sku": "sku",
        "category": "categories",
        "image_urls": "images",  # turn list of str into list of obj w/str
    }

    products = dict(
        (mapp[key], value) for (key, value) in record.items() if key in mapp.keys()
    )

    if products.get("images"):
        products["images"] = [{"src": i} for i in products["images"]]

    if products.get("categories"):
        products["categories"] = products["categories"]["name"]

    if not isinstance(products.get("regular_price"), str):
        products["regular_price"] = str(products["regular_price"])

    return products


def orders_from_unified(record):

    mapp = {
        "billing_address": "billing",
        "shipping_address": "shipping",
        "line_items": "line_items",
        "currency": "currency",
        "status": "status",
        "total_price": "total",
        "total_tax": "total_tax",
    }

    orders = dict(
        (mapp[key], value) for (key, value) in record.items() if key in mapp.keys()
    )

    if orders.get("line_items"):
        orders["line_items"] = line_items_from_unified(orders["line_items"])

    if orders.get("billing"):
        orders["billing"] = address_from_unified(orders["billing"])

    if orders.get("shipping"):
        orders["shipping"] = address_from_unified(orders["shipping"])

    return orders


def line_items_from_unified(line_items):

    mapp = {
        "product_id": "product_id",
        "product_name": "product_name",
        "total_price": "total",
        "quantity": "quantity",
    }
    items = []
    for item in line_items:
        items.append(
            dict(
                (mapp[key], value)
                for (key, value) in item.items()
                if key in mapp.keys()
            )
        )

    return items


def address_from_unified(address):

    mapp = {
        "line1": "address_1",
        "line2": "address_2",
        "city": "city",
        "state": "state",
        "postal_code": "postcode",
        "country": "country",
        "firstName": "first_name",
        "lastName": "last_name",
    }

    addres = dict(
        (mapp[key], value) for (key, value) in address.items() if key in mapp.keys()
    )

    return addres
