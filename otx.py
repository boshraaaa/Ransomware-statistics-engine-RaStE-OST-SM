import os
import requests
import json
import datetime
import csv
from configparser import ConfigParser


class OTX_Siphon(object):
    def __init__(self, config_path=None, days=30):
        self.config = self.load_config(config_path)  # Load configuration

        self.otx_api_key = self.config.get('otx', 'otx_api_key')  # Set AlienVault API key
        self.otx_url = self.config.get('otx', 'otx_url')  # Set AlienVault URL

        self.proxies = {
            'http': self.config.get('proxy', 'http'),  # Set HTTP proxy if present in config file
            'https': self.config.get('proxy', 'https')  # Set HTTPS proxy if present in config file
        }

        self.modified_since = datetime.datetime.now() - datetime.timedelta(days=days)
        print(f'Searching for ransomware-related pulses modified in the last {days} days.')

    def execute(self):
        # Open a CSV file to save all pulse details
        with open("ransomware_pulses_detailed.csv", 'w', newline='', encoding='utf-8') as resultFile:
            # Define CSV writer and headers
            csv_writer = csv.writer(resultFile, dialect='excel')
            headers = [
                "Pulse ID", "Pulse Name", "Created", "Modified", "Description",
                "Author Name", "Tags", "Indicator Type", "Indicator",
                "Title", "Source", "Reference", "Content", "Indicator Details"
            ]
            csv_writer.writerow(headers)

            # Retrieve and process pulses
            for pulse in self.get_pulse_generator():
                # General pulse data
                pulse_id = pulse.get("id", "N/A")
                pulse_name = pulse.get("name", "N/A")
                created = pulse.get("created", "N/A")
                modified = pulse.get("modified", "N/A")
                description = pulse.get("description", "N/A")
                author_name = pulse.get("author_name", "N/A")
                tags = ", ".join(pulse.get("tags", []))

                # Iterate over each indicator in the pulse
                for indicator in pulse.get("indicators", []):
                    indicator_type = indicator.get("type", "N/A")
                    indicator_value = indicator.get("indicator", "N/A")
                    title = indicator.get("title", "N/A")
                    source = indicator.get("source", "N/A")
                    reference = indicator.get("references", ["N/A"])[0]
                    content = indicator.get("content", "N/A")
                    indicator_details = json.dumps(indicator, indent=2)

                    # Write all details to the CSV file
                    csv_writer.writerow([
                        pulse_id, pulse_name, created, modified, description,
                        author_name, tags, indicator_type, indicator_value,
                        title, source, reference, content, indicator_details
                    ])
                    print(f"Logged Indicator: {indicator_value}")

        print("Data saved to ransomware_pulses_detailed.csv")

    def load_config(self, config_path):
        """Load and parse the configuration file."""
        if not config_path:
            config_path = os.path.join(os.path.expanduser('~'), '.otx_config')  # Default location

        try:
            config = ConfigParser()
            config.read(config_path)
            if not config.sections():
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            return config
        except Exception as e:
            raise Exception(f"Error loading config: {e}")

    def otx_get(self, url):
        """Make a GET request to AlienVault and return the response."""
        headers = {
            'X-OTX-API-KEY': self.otx_api_key,
        }

        r = requests.get(url, headers=headers, proxies=self.proxies, verify=True)
        if r.status_code == 200:
            return r.text
        else:
            print('Error retrieving AlienVault OTX data.')
            print(f'Status code was: {r.status_code}')
            return None

    def get_pulse_generator(self):
        """Retrieve and yield ransomware-related pulses."""
        args = [
            f'modified_since={self.modified_since.strftime("%Y-%m-%d %H:%M:%S.%f")}',
            'limit=10',
            'page=1'
        ]
        request_args = '?' + '&'.join(args)

        response_data = self.otx_get(f'{self.otx_url}/pulses/subscribed{request_args}')
        while response_data:
            all_pulses = json.loads(response_data)
            if 'results' in all_pulses:
                for pulse in all_pulses['results']:
                    if "ransomware" in pulse.get("tags", []):  # Filter by 'ransomware' tag
                        yield pulse
            response_data = None
            if 'next' in all_pulses and all_pulses['next']:
                response_data = self.otx_get(all_pulses['next'])


if __name__ == '__main__':
    # Set the path to the configuration file
    CONFIG_PATH = ".otx_config"  # Replace with your actual config file path
    DAYS = 30 # Number of days to look back for pulses

    siphon = OTX_Siphon(config_path=CONFIG_PATH, days=DAYS)
    siphon.execute()
