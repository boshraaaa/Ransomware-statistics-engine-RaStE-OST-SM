import os
import requests
import json
import csv
import datetime
from configparser import ConfigParser


class OTX_Siphon(object):
    def __init__(self, config_path=None, days=30):
        self.config = self.load_config(config_path)
        self.otx_api_key = self.config.get('otx', 'otx_api_key')
        self.otx_url = self.config.get('otx', 'otx_url')

        self.proxies = {
            'http': self.config.get('proxy', 'http'),
            'https': self.config.get('proxy', 'https')
        }

        self.modified_since = datetime.datetime.now() - datetime.timedelta(days=days)
        print(f"Searching for ransomware-related pulses modified in the last {days} days.")

    def execute(self):
        with open("detailed_ransomware_indicators.csv", 'w', newline='', encoding='utf-8') as result_file:
            csv_writer = csv.writer(result_file, dialect='excel')
            
            # Headers for the CSV file
            headers = [
                "Pulse ID", "Pulse Name", "Indicator Type", "Indicator",
                "Verdict", "Location", "ASN", "DNS Resolutions", 
                "Associated URLs", "Passive DNS", "Open Ports", 
                "Related Pulses", "Related Tags", "Analysis Summary",
                "External Resources", "Other Details"
            ]
            csv_writer.writerow(headers)

            # Fetch ransomware-related pulses
            for pulse in self.get_pulse_generator():
                pulse_id = pulse.get("id", "N/A")
                pulse_name = pulse.get("name", "N/A")

                for indicator in pulse.get("indicators", []):
                    indicator_type = indicator.get("type", "N/A")
                    indicator_value = indicator.get("indicator", "N/A")

                    # Fetch detailed data for the indicator
                    indicator_details = self.get_indicator_details(indicator_value, indicator_type)

                    csv_writer.writerow([
                        pulse_id, pulse_name, indicator_type, indicator_value,
                        indicator_details.get("verdict", "N/A"),
                        indicator_details.get("location", "N/A"),
                        indicator_details.get("asn", "N/A"),
                        indicator_details.get("dns_resolutions", "N/A"),
                        indicator_details.get("associated_urls", "N/A"),
                        indicator_details.get("passive_dns", "N/A"),
                        indicator_details.get("open_ports", "N/A"),
                        indicator_details.get("related_pulses", "N/A"),
                        indicator_details.get("related_tags", "N/A"),
                        indicator_details.get("analysis_summary", "N/A"),
                        indicator_details.get("external_resources", "N/A"),
                        json.dumps(indicator_details.get("other_details", {}), indent=2)
                    ])
                    print(f"Logged detailed data for indicator: {indicator_value}")

        print("Data saved to detailed_ransomware_indicators.csv")

    def load_config(self, config_path):
        """Load configuration file."""
        if not config_path:
            config_path = os.path.join(os.path.expanduser('~'), '.otx_config')
        try:
            config = ConfigParser()
            config.read(config_path)
            if not config.sections():
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            return config
        except Exception as e:
            raise Exception(f"Error loading config: {e}")

    def otx_get(self, url):
        """Make a GET request to OTX API."""
        headers = {
            'X-OTX-API-KEY': self.otx_api_key,
        }

        response = requests.get(url, headers=headers, proxies=self.proxies, verify=True)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error retrieving data from {url}. Status code: {response.status_code}")
            return None

    def get_pulse_generator(self):
        """Retrieve ransomware-related pulses."""
        args = [
            f"modified_since={self.modified_since.strftime('%Y-%m-%d %H:%M:%S.%f')}",
            "limit=10",
            "page=1"
        ]
        request_args = '?' + '&'.join(args)

        response_data = self.otx_get(f"{self.otx_url}/pulses/subscribed{request_args}")
        while response_data:
            if "results" in response_data:
                for pulse in response_data["results"]:
                    if "ransomware" in pulse.get("tags", []):  # Filter by 'ransomware' tag
                        yield pulse
            if "next" in response_data and response_data["next"]:
                response_data = self.otx_get(response_data["next"])
            else:
                response_data = None

    def get_indicator_details(self, indicator, indicator_type):
        """Fetch detailed information for a given indicator."""
        url = f"{self.otx_url}/indicators/{indicator_type}/{indicator}/general"
        data = self.otx_get(url)

        if not data:
            return {}

        # Extract detailed information from the response
        details = {
            "verdict": data.get("analysis", {}).get("general", {}).get("verdict", "N/A"),
            "location": data.get("geo", {}).get("country_name", "N/A"),
            "asn": data.get("geo", {}).get("asn", "N/A"),
            "dns_resolutions": ", ".join([dns.get("hostname", "N/A") for dns in data.get("passive_dns", [])]),
            "associated_urls": ", ".join([url.get("url", "N/A") for url in data.get("url_list", {}).get("url_list", [])]),
            "passive_dns": json.dumps(data.get("passive_dns", []), indent=2),
            "open_ports": ", ".join([str(port) for port in data.get("open_ports", [])]),
            "related_pulses": ", ".join([pulse.get("name", "N/A") for pulse in data.get("related_pulses", [])]),
            "related_tags": ", ".join(data.get("tags", [])),
            "analysis_summary": data.get("analysis", {}).get("general", {}).get("summary", "N/A"),
            "external_resources": ", ".join(data.get("external_references", [])),
            "other_details": data  # Include all other details as JSON
        }
        return details


if __name__ == '__main__':
    CONFIG_PATH = ".otx_config"  # Replace with your actual config file path
    DAYS = 30  # Number of days to look back for pulses

    siphon = OTX_Siphon(config_path=CONFIG_PATH, days=DAYS)
    siphon.execute()
