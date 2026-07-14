import os
import time
import importlib

if __name__ == "__main__":

    start_time = time.time()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    dataset_dir = os.path.join(script_dir, "..", "3_datasets")
    os.makedirs(dataset_dir, exist_ok=True)

    print("=" * 60)
    print("Restaurant Analytics Synthetic Data Generation")
    print("=" * 60)

    # -------------------------------------------------
    # Generate Master Data
    # -------------------------------------------------
    print("\nGenerating Master Data...")

    master_data = importlib.import_module("00_master_data")
    master_data.generate_data_for_sql_db()

    print("✓ Master Data Generated")

    # -------------------------------------------------
    # Generate Historical Orders
    # -------------------------------------------------
    print("\nGenerating Historical Orders...")

    historical_orders = importlib.import_module("01_historical_orders")
    historical_orders.generate_historical_orders(
        num_orders=8000,
        months_back=6
    )

    print("✓ Historical Orders Generated")

    # -------------------------------------------------
    # Generate Customer Reviews
    # -------------------------------------------------
    print("\nGenerating Customer Reviews...")

    reviews = importlib.import_module("02_reviews")
    reviews.generate_customer_reviews(
        review_percentage=0.35
    )

    print("✓ Customer Reviews Generated")

    elapsed = time.time() - start_time

    print("\n" + "=" * 60)
    print("Synthetic Data Generation Completed Successfully")
    print("=" * 60)
    print(f"Output Folder : {dataset_dir}")
    print(f"Execution Time: {elapsed:.2f} seconds")