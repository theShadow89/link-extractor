from urllib.parse import urlparse, urljoin

from bs4 import BeautifulSoup


class LinkExtractor:

    def is_external_link(self, source_url, link_url):
        source_domain = urlparse(source_url).netloc
        link_domain = urlparse(link_url).netloc
        return source_domain != link_domain and link_domain != ''

    def extract_links(self, record):
        links = {}

        if record.rec_type == 'response':
            try:
                url = record.rec_headers.get_header('WARC-Target-URI')
                if url not in links:
                    links[url] = set()

                content = record.content_stream().read()
                soup = BeautifulSoup(content, 'html.parser')

                for a_tag in soup.find_all('a', href=True):
                    href = a_tag['href']
                    absolute_url = urljoin(url, href)
                    if self.is_external_link(url, absolute_url):
                        links[url].add(absolute_url)
            except Exception as e:
                print(f"Error processing {url}: {str(e)}")

        # Convert sets to lists for JSON serialization
        return {k: list(v) for k, v in links.items()}
