import os
import sys
from urllib.parse import urlparse, urljoin
from warcio.archiveiterator import ArchiveIterator

from extract_links import LinkExtractor
from link_aggregator import LinksAggregator
from metrics_calculator import MetricsCalculator
from storage import ExternalLinkStorage, LinkData


class LinksPipeline:
    def __init__(self, data_directory):
        self.data_directory = data_directory
        self.links_aggregator = LinksAggregator()
        self.metrics_calculator = MetricsCalculator()

    # Function to check if the link is the homepage
    def is_homepage(self, url):
        parsed_url = urlparse(url)
        # The homepage is when there's no path after the domain
        return parsed_url.path in ['', '/', None]

    def process_warc_files(self):
        le = LinkExtractor()
        storage = ExternalLinkStorage()
        for filename in os.listdir(self.data_directory):
            if filename.endswith('.warc.gz'):
                print(f"Processing {filename}")
                with open(os.path.join(self.data_directory, filename), 'rb') as stream:
                    for record in ArchiveIterator(stream):
                        page_external_links = le.extract_links(record)
                        if len(page_external_links) > 0:
                            for page, links in page_external_links.items():
                                links_data = [LinkData(url, self.is_homepage(url)) for url in links]
                                storage.insert_link(links_data)

    def run(self):
        self.process_warc_files()
        print(f"Extracted links saved")

        try:
            print("Aggregating links...")
            self.links_aggregator.aggregate_links()
            print("Aggregation complete")
        except Exception as e:
            print(f"An error occurred: {e}")

        try:
            metrics = self.metrics_calculator.compute_metrics()
            print("Metrics computed successfully:")

            # Define partition columns
            partition_cols = ['date']

            # Save metrics to Arrow format
            self.metrics_calculator.save_metrics_to_arrow(metrics, partition_cols)
            print("Metrics saved successfully in Arrow format with partitioning.")
        except Exception as e:
            print(f"An error occurred: {e}")


def main():
    print(sys.argv)
    warc_directory = sys.argv[1]  # Update this path

    p = LinksPipeline(warc_directory)

    try:
        p.run()
    except Exception as e:
        print(f"Error accured {e}")


if __name__ == "__main__":
    main()
