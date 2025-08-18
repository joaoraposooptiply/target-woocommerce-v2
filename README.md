# target-woocommerce

`target-woocommerce` is a Singer target for WooCommerce.

Build with the [Meltano Target SDK](https://sdk.meltano.com).

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install target-woocommerce
```

## Configuration

### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the target.

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-woocommerce --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

- [ ] `Developer TODO:` If your target requires special access on the source system, or any special authentication requirements, provide those here.

## Usage

You can easily run `target-woocommerce` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Target Directly

```bash
target-woocommerce --version
target-woocommerce --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-woocommerce --config /path/to/target-woocommerce-config.json
```

## Error Handling

This target implements robust error handling to ensure that processing continues even when individual records fail. The following error handling features are implemented:

### Record-Level Error Handling
- **Failed Records**: When a record fails to process (e.g., product not found, API errors), the target logs the error and continues with the next record instead of stopping the entire pipeline.
- **Missing Products**: If a product referenced in an order or inventory update cannot be found, the target logs a warning and skips that specific line item or record.
- **API Failures**: Network errors, authentication failures, and other API-related issues are caught and logged without stopping the pipeline.

### Error Logging
- All errors are logged with detailed information including the record data and specific error messages.
- Failed records return a tuple indicating failure: `(None, False, {"error": "error_message"})`
- Successful records return: `(id, True, {"updated": True})` or `(id, True, {})`

### Supported Streams
The target supports the following streams with error handling:
- **SalesOrders**: Order creation and updates
- **UpdateInventory**: Product inventory updates
- **Products**: Product creation and updates
- **OrderNotes**: Order note creation

### Example Error Handling Behavior
If processing 10 records and record #5 fails:
- **Before**: Processing stops at record #5, records #6-10 are not processed
- **After**: Record #5 is logged as failed, processing continues with records #6-10

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

```bash
# Run tests
poetry run pytest

# Run error handling tests
python target_woocommerce/test_error_handling.py
```

### Testing Error Handling

To test the error handling functionality:

```bash
# Run the error handling test
python target_woocommerce/test_error_handling.py
```

This will simulate various failure scenarios and verify that the target continues processing instead of stopping on errors.
