"""WoocommerceSink target sink class, which handles writing streams."""


from datetime import datetime
from typing import Dict, List, Optional


from singer_sdk.plugin_base import PluginBase
from target_hotglue.client import HotglueSink
from base64 import b64encode
from random_user_agent.user_agent import UserAgent
import requests

MAX_PARALLELISM = 10

class WoocommerceSink(HotglueSink):
    """WoocommerceSink target sink class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        self._state = dict(target._state)
        # Initialize export statistics
        self.export_stats = {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "errors": []
        }
        super().__init__(target, stream_name, schema, key_properties)

    summary_init = False
    available_names = []

    user_agents = UserAgent(software_engines="blink", software_names="chrome")

    @property
    def name(self):
        raise NotImplementedError

    @property
    def endpoint(self):
        raise NotImplementedError

    @property
    def unified_schema(self):
        raise NotImplementedError
    @property 
    def user_agent(self):
        # Woocom throws 403 if the user agent is not set
        if self.config.get("user_agent"):
            return self.config.get("user_agent")
        return self.user_agents.get_random_user_agent().strip()
    @property
    def authenticator(self):
        user = self.config.get("consumer_key")
        passwd = self.config.get("consumer_secret")
        token = b64encode(f"{user}:{passwd}".encode()).decode()
        return f"Basic {token}"

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["Content-Type"] = "application/json"
        headers.update({"Authorization": self.authenticator})
        
        headers["User-Agent"] = self.user_agent
        return headers

    @property
    def base_url(self):
        site_url = self.config["site_url"].strip("/")
        return f"{site_url}/wp-json/wc/v3/"

    def url(self, endpoint=None):
        if not endpoint:
            endpoint = self.endpoint
        return f"{self.base_url}{endpoint}"

    def validate_input(self, record: dict):
        return self.unified_schema(**record).dict()

    def validate_output(self, mapping):
        payload = self.clean_payload(mapping)
        # Add validation logic here
        return payload

    def check_payload_for_fields(self, payload, fields):
        return all([field in payload for field in fields])

    def get_if_missing_fields(self, response, fields, fallback_url):
        try:
            if fallback_url is None:
                return response

            modified_response = []
            for resp in response:
                try:
                    if not self.check_payload_for_fields(resp, fields):
                        modified_response.append(
                            self.request_api("GET", f"{fallback_url}{resp['id']}").json()
                        )
                    else:
                        modified_response.append(resp)
                except Exception as e:
                    self.logger.error(f"Failed to get missing fields for response {resp.get('id', 'unknown')}: {str(e)}")
                    # Keep the original response if we can't get the missing fields
                    modified_response.append(resp)
            return modified_response
        except Exception as e:
            self.logger.error(f"Failed to get missing fields: {str(e)}")
            # Return original response if we can't process missing fields
            return response

    def get_reference_data(self, stream, fields=None, filter={}, fallback_url=None):
        try:
            self.logger.info(f"Getting reference data for {stream}")
            page = 1
            data = []
            params = {"per_page": 100, "order": "asc", "page": page}
            params.update(filter)
            while True:
                try:
                    resp = self.request_api("GET", stream, params)
                    total_pages = resp.headers.get("X-WP-TotalPages")
                    resp = resp.json()
                    if fields:
                        resp = [
                            {k: v for k, v in r.items() if k in fields}
                            for r in self.get_if_missing_fields(resp, fields, fallback_url)
                        ]
                    data += resp

                    if page % 10 == 0:
                        self.logger.info(f"Fetched {len(data)} records for {stream} page {page}")

                    if resp and int(total_pages) > page:
                        page += 1
                        params.update({"page": page})
                    else:
                        break
                except Exception as e:
                    self.logger.error(f"Failed to fetch page {page} for {stream}: {str(e)}")
                    # Continue with next page instead of stopping
                    break
                    
            self.logger.info(f"Reference data for {stream} fetched successfully. {len(data)} records found.")
            return data
        except Exception as e:
            self.logger.error(f"Failed to get reference data for {stream}: {str(e)}")
            # Return empty list instead of raising exception
            return []

    def report_success(self, record_id: str, operation: str = "processed"):
        """Report successful record processing."""
        self.export_stats["successful_records"] += 1
        self.logger.info(f"SUCCESS: {self.name} record {record_id} {operation} successfully")
        import sys
        print(f"SUCCESS: {self.name} record {record_id} {operation} successfully", file=sys.stderr)

    def report_failure(self, error_message: str, record_data: dict = None):
        """Report failed record processing."""
        self.export_stats["failed_records"] += 1
        self.export_stats["errors"].append({
            "message": error_message,
            "record": record_data
        })
        self.report_error_to_job(error_message, record_data)

    def report_export_summary(self):
        """Report export summary with statistics."""
        total = self.export_stats["total_records"]
        successful = self.export_stats["successful_records"]
        failed = self.export_stats["failed_records"]
        success_rate = (successful / total * 100) if total > 0 else 0
        
        summary = f"""
=== EXPORT SUMMARY FOR {self.name} ===
Total Records: {total}
Successful: {successful}
Failed: {failed}
Success Rate: {success_rate:.1f}%
"""
        
        if failed > 0:
            summary += f"\nErrors encountered: {len(self.export_stats['errors'])}"
            for i, error in enumerate(self.export_stats['errors'][:5], 1):  # Show first 5 errors
                summary += f"\n  {i}. {error['message']}"
            if len(self.export_stats['errors']) > 5:
                summary += f"\n  ... and {len(self.export_stats['errors']) - 5} more errors"
        
        self.logger.info(summary)
        import sys
        print(summary, file=sys.stderr)

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

    def request_api(self, method, endpoint=None, params=None, request_data=None):
        """Make API request with error handling."""
        try:
            url = self.url(endpoint)
            headers = self.http_headers
            
            if method.upper() == "GET":
                response = requests.get(url, headers=headers, params=params)
            elif method.upper() == "POST":
                response = requests.post(url, headers=headers, json=request_data, params=params)
            elif method.upper() == "PUT":
                response = requests.put(url, headers=headers, json=request_data, params=params)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")
            
            # Check for HTTP errors
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            error_msg = f"API request failed: {method} {url}"
            self.report_error_to_job(error_msg)
            self.logger.error(f"Error: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                self.logger.error(f"Response status: {e.response.status_code}")
                self.logger.error(f"Response content: {e.response.text}")
            raise
        except Exception as e:
            error_msg = f"Unexpected error in API request: {str(e)}"
            self.report_error_to_job(error_msg)
            raise

    @staticmethod
    def clean_dict_items(dict):
        return {k: v for k, v in dict.items() if v not in [None, ""]}

    def clean_payload(self, item):
        item = self.clean_dict_items(item)
        output = {}
        for k, v in item.items():
            if isinstance(v, datetime):
                dt_str = v.strftime("%Y-%m-%dT%H:%M:%S%z")
                if len(dt_str) > 20:
                    output[k] = f"{dt_str[:-2]}:{dt_str[-2:]}"
                else:
                    output[k] = dt_str
            elif isinstance(v, dict):
                output[k] = self.clean_payload(v)
            else:
                output[k] = v
        return output
