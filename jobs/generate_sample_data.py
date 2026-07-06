from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw"
fake = Faker("ru_RU")


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    random.seed(42)

    customers = [
        {
            "customer_id": i,
            "customer_name": fake.name(),
            "region": random.choice(
                ["Moscow", "Saint Petersburg", "Kazan", "Novosibirsk", "Yekaterinburg"]
            ),
            "registration_date": (
                datetime(2024, 1, 1) + timedelta(days=random.randint(0, 120))
            )
            .date()
            .isoformat(),
        }
        for i in range(1, 501)
    ]

    categories = ["Electronics", "Furniture", "Home", "Stationery", "Sports"]
    products = [
        {
            "product_id": i,
            "product_name": f"{random.choice(categories)} Item {i}",
            "category": random.choice(categories),
            "base_price": round(random.uniform(300, 25000), 2),
        }
        for i in range(1001, 1101)
    ]

    orders = []
    events = []
    start = datetime(2024, 5, 1)

    for order_id in range(1, 10001):
        customer = random.choice(customers)
        product = random.choice(products)
        quantity = random.randint(1, 5)
        order_date = start + timedelta(days=random.randint(0, 60))
        status = random.choices(["paid", "cancelled", "refunded"], weights=[88, 8, 4])[0]
        total_amount = round(float(product["base_price"]) * quantity, 2)

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "product_id": product["product_id"],
                "order_date": order_date.date().isoformat(),
                "quantity": quantity,
                "total_amount": total_amount,
                "status": status,
            }
        )

        events.append(
            {
                "event_id": order_id * 3 - 2,
                "customer_id": customer["customer_id"],
                "event_time": (order_date - timedelta(minutes=8)).strftime("%Y-%m-%d %H:%M:%S"),
                "event_type": "view",
                "product_id": product["product_id"],
            }
        )
        events.append(
            {
                "event_id": order_id * 3 - 1,
                "customer_id": customer["customer_id"],
                "event_time": (order_date - timedelta(minutes=4)).strftime("%Y-%m-%d %H:%M:%S"),
                "event_type": "add_to_cart",
                "product_id": product["product_id"],
            }
        )
        if status == "paid":
            events.append(
                {
                    "event_id": order_id * 3,
                    "customer_id": customer["customer_id"],
                    "event_time": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                    "event_type": "purchase",
                    "product_id": product["product_id"],
                }
            )

    write_csv(RAW_DIR / "customers.csv", customers)
    write_csv(RAW_DIR / "products.csv", products)
    write_csv(RAW_DIR / "orders.csv", orders)
    write_csv(RAW_DIR / "events.csv", events)
    print(f"Sample data generated in {RAW_DIR}")


if __name__ == "__main__":
    main()
