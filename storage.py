import json
from typing import List

import psycopg2
from psycopg2.extras import execute_values


class LinkData:
    def __init__(self, url: str, is_homepage: bool = False):
        self.url = url
        self.is_homepage = is_homepage


class ExternalLinkStorage:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="crawl_data",
            user="crawl_user",
            password="crawl_password",
            host="localhost",
            port="5432"
        )

    def insert_link(self, links: List[LinkData]):
        with self.conn.cursor() as cur:
            execute_values(cur,
                           "INSERT INTO external_links (link, is_homepage) VALUES %s",
                           [(link_data.url, link_data.is_homepage) for link_data in links],
                           page_size=1000
                           )
        self.conn.commit()

    def select_all(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT link, is_homepage FROM external_links")
            for row in cur:
                yield row


class AggregateLinkStorage:
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname="crawl_data",
            user="crawl_user",
            password="crawl_password",
            host="localhost",
            port="5432"
        )

    def fetch_all(self):
        with self.conn.cursor() as cur:
            # Fetch all data
            cur.execute("""
                SELECT primary_link, frequency, country, category, is_ad_based
                FROM aggregated_links
            """)
            data = cur.fetchall()
        return data

    def insert_aggregated_data(self, data):
        with self.conn.cursor() as cur:
            execute_values(cur,
                           """
                           INSERT INTO aggregated_links (primary_link, frequency, subsections, country, category, is_ad_based)
                           VALUES %s
                           """,
                           [(
                               primary_link,
                               info["frequency"],
                               json.dumps({k: v for k, v in info["subsections"].items() if k}),
                               # Exclude empty subsections
                               info["country"],
                               info["category"],
                               info["is_ad"],
                           ) for primary_link, info in data.items()],
                           page_size=1000
                           )
        self.conn.commit()
