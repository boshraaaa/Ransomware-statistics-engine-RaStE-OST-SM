#!/usr/bin/env python

# OTX_Siphon
# Pulls pulses from Alien Vault subscription list
# Using AlienVault SDK for OTXv2 https://github.com/AlienVault-OTX/OTX-Python-SDK

import os
import requests
import json
import datetime
import csv
from configparser import ConfigParser


class OTX_Siphon(object):
    def __init__(self, config_path, days=None):
        self.config = self.load_config(config_path)  # Load configuration

        self.otx_api_key = self.config.get('otx', 'otx_api_key')  # Set AlienVault API key
        self.otx_url = self.config.get('otx', 'otx_url')  # Set AlienVault URL

        self.proxies = {
            'http': self.config.get('proxy', 'http'),  # Set HTTP proxy if present in config file
            'https': self.config.get('proxy', 'https')  # Set HTTPS proxy if present in config file
        }

        self.modified_since = None  # Set pulse range to those modified in last x days
        if days:
            print(f'Searching for pulses modified in the last {days} days')
            self.modified_since = datetime.datetime.now() - datetime.timedelta(days=days)

    def execute(self):
        for pulse in self.get_pulse_generator(modified_since=self.modified_since, proxies=self.proxies):
            print(f'Found pulse with ID {pulse["id"]} and title {pulse["name"].encode("utf-8")}')

            indicator_data = pulse['indicators']  # Pull indicators from pulse
            event_title = pulse['name']  # Pull title from pulse
            created = pulse['created']  # Pull date/time from pulse
            reference = pulse.get('reference', ['No reference documented'])[0]

            print('Pulse name:', event_title)
            print('Created:', created)

            with open("output.csv", 'w', newline='') as resultFile:
                wr = csv.writer(resultFile, dialect='excel')
                for i in pulse['indicators']:
                    result = [event_title, created, i['type'], i['indicator'], reference]
                    print('Indicator data:', str(indicator_data))
                    print('--------------------------------------')
                    print(result)
                    wr.writerow(result)

    def parse_config(self, location):
        """Parses the OTX config file from the given location."""
        try:
            config = ConfigParser()
            config.read(location)
        except Exception as e:
            print(f'Error parsing config: {e}')
            return False
        if len(config.sections()) == 0:
            print(f'Configuration file not found: {location}')
            return False
        return config

    def load_config(self, config_path):
        """Loads and returns the parsed configuration file."""
        return self.parse_config(config_path)

    def otx_get(self, url, proxies=None, verify=True):
        """Build headers and issue a get request to AlienVault and return response data."""
        headers = {
            'X-OTX-API-KEY': self.otx_api_key,
        }

        r = requests.get(url, headers=headers, proxies=proxies, verify=verify)
        if r.status_code == 200:
            return r.text
        else:
            print('Error retrieving AlienVault OTX data.')
            print(f'Status code was: {r.status_code}')
            return False

    def get_pulse_generator(self, modified_since=None, proxies=None, verify=True):
        """Accept, loop/parse, and store pulse data."""
        args = []

        if modified_since:
            args.append(f'modified_since={modified_since.strftime("%Y-%m-%d %H:%M:%S.%f")}')

        args.append('limit=10')
        args.append('page=1')
        request_args = '&'.join(args)
        request_args = f'?{request_args}'

        response_data = self.otx_get(f'{self.otx_url}/pulses/subscribed{request_args}', proxies=proxies, verify=verify)
        while response_data:
            all_pulses = json.loads(response_data)
            if 'results' in all_pulses:
                for pulse in all_pulses['results']:
                    yield pulse
            response_data = None
            if 'next' in all_pulses and all_pulses['next']:
                response_data = self.otx_get(all_pulses['next'], proxies=proxies, verify=verify)


def main():
    config_path = os.path.join(os.getcwd(), '.otx_config')  
    days = 30

    siphon = OTX_Siphon(config_path=config_path, days=days)
    siphon.execute()


if __name__ == '__main__':
    main()
