from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_product_category_view(
    products: DataFrame,
    categories: DataFrame,
    product_categories: DataFrame,
) -> DataFrame:
    """
    Возвращает датафрейм, содержащий
    • все пары (product_name, category_name);
    • те же product_name, у которых нет категорий (category_name = NULL).

    Columns in:
        products:          product_id, product_name
        categories:        category_id, category_name
        product_categories product_id, category_id
    """
    # ―― связываем продукты → категории через таблицу связей
    result = (
        products.alias("p")
        .join(product_categories.alias("pc"),
              F.col("p.product_id") == F.col("pc.product_id"),
              how="left")                                   # оставляем ВСЕ продукты
        .join(categories.alias("c"),
              F.col("pc.category_id") == F.col("c.category_id"),
              how="left")                                   # категории могут отсутствовать
        .select(
            F.col("p.product_name").alias("product_name"),
            F.col("c.category_name").alias("category_name"),
        )
    )

    return result
