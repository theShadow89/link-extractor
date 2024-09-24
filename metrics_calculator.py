from collections import Counter
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from storage import AggregateLinkStorage


class MetricsCalculator:

    def __init__(self):
        self.aggregate_link_storage = AggregateLinkStorage()

    def compute_metrics(self, conn):

        data = self.aggregate_link_storage.fetch_all()
        df = pd.DataFrame(data, columns=['primary_link', 'frequency', 'country', 'category', 'is_ad_based'])

        metrics = {}

        # 1. Top 10 most frequent domains
        metrics['top_10_domains'] = df.nlargest(10, 'frequency')[['primary_link', 'frequency']].to_dict('records')

        # 2. Distribution of websites by country
        metrics['country_distribution'] = df['country'].value_counts().to_dict()

        # 3. Percentage of ad-based domains
        metrics['ad_based_percentage'] = (df['is_ad_based'].sum() / len(df)) * 100

        # 4. Top 5 categories (for categorized websites)
        category_counts = Counter()
        for categories in df['category'].dropna():
            category_counts.update([cat['id'] for cat in categories])
        metrics['top_5_categories'] = dict(category_counts.most_common(5))

        # 5. Average number of subcategories per categorized website
        df['subcategory_count'] = df['category'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        metrics['avg_subcategories'] = df['subcategory_count'].mean()

        return metrics

    def flatten_metrics(metrics):
        flattened = {}
        for key, value in metrics.items():
            if isinstance(value, dict):
                for subkey, subvalue in value.items():
                    flattened[f"{key}_{subkey}"] = subvalue
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    for subkey, subvalue in item.items():
                        flattened[f"{key}_{i}_{subkey}"] = subvalue
            else:
                flattened[key] = value
        return flattened

    def save_metrics_to_arrow(self, metrics, partition_cols):
        # Flatten the metrics dictionary
        flat_metrics = self.flatten_metrics(metrics)

        # Create a DataFrame from the flattened metrics
        df = pd.DataFrame([flat_metrics])

        # Add partition columns
        df['date'] = datetime.now().strftime('%Y-%m-%d')
        df['hour'] = datetime.now().strftime('%H')

        # Convert DataFrame to Arrow Table
        table = pa.Table.from_pandas(df)

        # Write to Parquet dataset with partitioning
        pq.write_to_dataset(
            table,
            root_path="metrics_dataset",
            partition_cols=partition_cols,
            existing_data_behavior="overwrite_or_ignore"
        )


def main():
    metrics_calculator = MetricsCalculator()
    try:
        metrics = metrics_calculator.compute_metrics()
        print("Metrics computed successfully:")

        # Define partition columns
        partition_cols = ['date', 'hour']

        # Save metrics to Arrow format
        metrics_calculator.save_metrics_to_arrow(metrics, partition_cols)
        print("Metrics saved successfully in Arrow format with partitioning.")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
