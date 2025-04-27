import sys
from pyspark.sql import SparkSession
from product_category_view import build_product_category_view
import tempfile, os, sys

def main() -> None:
    """Создаём примеры датафреймов и демонстрируем работу build_product_category_view."""
    spark = (
    SparkSession.builder
    .appName("Product-Category demo")
    .master("local[1]")                                # один поток достаточно
    # 👉 говорим Spark'у, какой Python брать
    .config("spark.pyspark.python", sys.executable)
    .config("spark.pyspark.driver.python", sys.executable)
    # 👉 слушаем только 127.0.0.1, чтобы не лез в IPv6/внешние IP
    .config("spark.eventLog.dir",    "file:///C:/tmp/spark-events")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host",       "127.0.0.1")
    .getOrCreate()
)

    # ---------------------------
    # 1. Игрушечные данные
    # ---------------------------
    products_data = [
        (1, "Шампунь"),
        (2, "Хлеб"),
        (3, "Молоко"),  # без категории
    ]
    categories_data = [
        (10, "Косметика"),
        (11, "Уход за волосами"),
        (12, "Выпечка"),
    ]
    product_categories_data = [
        (1, 10),
        (1, 11),
        (2, 12),
        # у продукта 3 связей нет
    ]

    products_df = spark.createDataFrame(products_data, ["product_id", "product_name"])
    categories_df = spark.createDataFrame(categories_data, ["category_id", "category_name"])
    product_categories_df = spark.createDataFrame(
        product_categories_data, ["product_id", "category_id"]
    )

    # ---------------------------
    # 2. Вызываем функцию
    # ---------------------------
    result_df = build_product_category_view(
        products_df, categories_df, product_categories_df
    )

    # ---------------------------
    # 3. Показываем результат
    # ---------------------------
    result_df.show(truncate=False)

    # ---------------------------
    # 4. Останавливаем Spark
    # ---------------------------
    spark.stop()


if __name__ == "__main__":
    print("TEMP =", tempfile.gettempdir())
    main()
    