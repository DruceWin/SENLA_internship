#!/usr/bin/env python
# coding: utf-8

import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql.connect.dataframe import DataFrame
from pyspark.sql.functions import when, sum, count, desc, first, countDistinct, lit
from datetime import datetime, date

from pyspark.sql.types import StructType

from schemas_and_date import schemes, date_used

SAVE_FOLDER = "result"


def timer(fn):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        print(f"\nНачало выполнения!\n{start_time}\n")
        fn(*args, **kwargs)
        finish_time = datetime.now()
        print(f"\nЗавершено!\n{finish_time}, (затраченное время - {finish_time - start_time})")
    return wrapper


def read_csv_to_df(spark_session: "SparkSession",
                   file_name: str,
                   schema: "StructType",
                   header: bool = True,
                   **kwargs) -> "DataFrame":
    """Считывает выбранный csv файл по предоставленной схеме и возвращает на выходе DataFrame"""
    return spark_session.read.csv(file_name, schema=schema, header=header, **kwargs)


def combine_dfs(transactions_df: "DataFrame",
                articles_df: "DataFrame",
                customers_df: "DataFrame",
                start_date: date,
                finish_date: date) -> "DataFrame":
    window_most_exp_art = (
        Window
        .partitionBy("customer_id")
        .orderBy(desc("price"), "t_dat")
    )

    return (
        transactions_df
        .filter(transactions_df.t_dat.between(start_date, finish_date))
        .join(articles_df.select("article_id", "product_group_name"), "article_id", "left")
        .withColumn("most_exp_id", first("article_id").over(window_most_exp_art))
        .groupBy("customer_id")
        .agg(
            sum("price").alias("transaction_amount"),
            first("most_exp_id").alias("most_exp_article_id"),
            count("*").alias("number_of_articles"),
            countDistinct("product_group_name").alias("number_of_product_groups")
        )
        .join(customers_df.select("customer_id", "age"), "customer_id", "left")
        .withColumn(
            "customer_group_by_age",
            when(customers_df["age"] < 23, "S")
            .when(customers_df["age"] > 59, "R")
            .otherwise("A")
        )
        .withColumn("part_date", lit(finish_date))
        .select("part_date", "customer_id", "customer_group_by_age", "transaction_amount",
                "most_exp_article_id", "number_of_articles", "number_of_product_groups")
    )


def save_df_to_csv(result_df: "DataFrame"):
    (
        result_df
        .repartition(1)
        .write
        .format("csv")
        .mode("overwrite")
        .option("header", "true")
        .save(SAVE_FOLDER)
    )


@timer
def main():
    spark = SparkSession.builder.master("local").getOrCreate()
    try:
        user_date = sys.argv[sys.argv.index("part_date") + 1]
        period = date_used.DatesUsed(**date_used.DatesUsed.get_year_and_month(user_date))
        print(f"Выбранный период: {period.start_period} -- {period.part_date}")

        print(f"Начато считывание файлов...", end="")
        articles_df = read_csv_to_df(spark, "articles.csv", schema=schemes.schema_articles)
        customers_df = read_csv_to_df(spark, "customers.csv", schema=schemes.schema_customers)
        transactions_df = read_csv_to_df(spark, "transactions_train.csv", schema=schemes.schema_transactions_train)
        print("OK")

        print("Начата обработка...", end="")
        result_df = combine_dfs(transactions_df=transactions_df, articles_df=articles_df,
                                customers_df=customers_df, start_date=period.start_period,
                                finish_date=period.part_date)
        print("OK")
        print(f"Начато сохранение результатов в папку {SAVE_FOLDER} ...")
        save_df_to_csv(result_df)
        print("Сохранено успешно!")
    except Exception as ex:
        print(f"Произошла ошибка: {ex}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
