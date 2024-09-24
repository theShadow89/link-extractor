import socket
from collections import defaultdict
from urllib.parse import urlparse

import requests

from storage import AggregateLinkStorage, ExternalLinkStorage


class LinksAggregator:

    def __init__(self):
        self.aggregate_link_storage = AggregateLinkStorage()
        self.link_storage = ExternalLinkStorage()

    def get_ip_from_url(self, url):
        parsed_url = urlparse(url)
        try:
            return socket.gethostbyname(parsed_url.netloc)
        except socket.gaierror:
            return None

    def get_country_from_ip(self, ip_address):
        if ip_address is None:
            return 'Unknown'

        response = requests.get(f"https://ipinfo.io/{ip_address}/json")
        if response.status_code == 200:
            data = response.json()
            return data.get('country', 'Unknown')
        return 'Unknown'

    def get_primary_link(self, url):
        parsed_url = urlparse(url)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    def get_subsection(self, url):
        parsed_url = urlparse(url)
        return parsed_url.path

    def categorize_website(self, url, api_key=None):
        # API endpoint
        api_endpoint = "https://website-categorization.whoisxmlapi.com/api/v2"

        if api_key is None:
            return None

        # Parameters for the API request
        params = {
            "apiKey": api_key,
            "url": url
        }

        try:
            # Make the API request
            response = requests.get(api_endpoint, params=params)
            response.raise_for_status()  # Raise an exception for bad status codes

            # Parse the JSON response
            data = response.json()

            # Extract categories
            categories = data.get("categories", [])

            # Find the category with the highest confidence score
            if categories:
                highest_confidence_category = max(categories, key=lambda x: x.get('confidence', 0))
                return highest_confidence_category['name']
            else:
                return None

        except requests.exceptions.RequestException as e:
            return None

    # URL of a public ad domain list (example using EasyList)
    AD_DOMAIN_LIST_URL = "https://easylist.to/easylist/easylist.txt"

    def download_ad_domain_list(self, ):
        response = requests.get(self.AD_DOMAIN_LIST_URL)
        if response.status_code == 200:
            return response.text.splitlines()
        else:
            print(f"Failed to download ad domain list: {response.status_code}")
            return []

    def is_ad_based_domain(self, domain, ad_domains, ad_keywords):
        # Check if domain is in the list of known ad domains
        if any(ad_domain in domain for ad_domain in ad_domains):
            return True

        # Check if domain contains common ad-related keywords
        if any(keyword in domain for keyword in ad_keywords):
            return True

        return False

    def aggregate_links(self):
        aggregated_data = defaultdict(
            lambda: {"frequency": 0, "subsections": defaultdict(int), "country": "Unknown", "category": None,
                     "is_ad": False})

        # Download and process ad domain list
        ad_domain_list = self.download_ad_domain_list()
        ad_domains = [line.strip('||').strip('^') for line in ad_domain_list if line.startswith('||') and '^' in line]

        # Common ad-related keywords
        ad_keywords = ['ad', 'ads', 'advert', 'advertising', 'banner', 'click', 'track', 'pixel']

        for row in self.link_storage.select_all():
            url = row[0]
            is_homepage = row[1]
            primary_link = self.get_primary_link(url)
            subsection = self.get_subsection(url)

            if is_homepage:
                try:
                    site_ip = self.get_ip_from_url(url)
                    country = self.get_country_from_ip(site_ip)
                    category = self.categorize_website(primary_link)
                    if category is None:
                        is_ad = self.is_ad_based_domain(primary_link, ad_domains, ad_keywords)

                    aggregated_data[primary_link]["country"] = country
                    aggregated_data[primary_link]["category"] = category
                    aggregated_data[primary_link]["country"] = is_ad

                except Exception:
                    continue
            else:
                aggregated_data[primary_link]["frequency"] += 1
                aggregated_data[primary_link]["subsections"][subsection] += 1

        self.aggregate_link_storage.insert_aggregated_data(aggregated_data)

        return aggregated_data


def main():
    links_aggregator = LinksAggregator()

    try:
        print("Aggregating links...")
        aggregated_data = links_aggregator.aggregate_links()
        print("Aggregation complete")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
