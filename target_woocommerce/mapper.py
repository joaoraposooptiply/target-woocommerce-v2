

def products_from_unified(record):

    mapp = {
        "name":"name",
        "variant":"type",
        "price":"regular_price",
        "description":"description",
        "short_description":"short_description",
        "sku":"sku",
        "category":"categories",
        "image_urls":"images", # turn list of str into list of obj w/str
        }

    products = dict(
        (mapp[key], value) for (key, value) in record.items() if key in mapp.keys()
    )

    if products.get("images"):
        products["images"] = [{"src":i} for i in products["images"]]

    if products.get("categories"):
        products["categories"] = products["categories"]["name"]

    if not isinstance(products.get("regular_price"),str):
        products["regular_price"] = str(products["regular_price"])

    return products