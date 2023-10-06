#!/usr/bin/env python
# coding: utf-8

import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType, DoubleType
from pyspark.sql.functions import when, sum, count, desc, first, countDistinct, lit

from datetime import date, timedelta, datetime


class DatesUsed:
    def __init__(self, year: int, month: int):
        self.year = year
        self.month = month

    @staticmethod
    def get_year_and_month(date_str: str) -> dict:
        """Преобразует из строки, подобной дате формата YYYY-MM, 
        словарь с годом и месяцем для создания экземпляра класса"""
        full_date = date_str.split("-")
        if len(full_date) < 2 or len(full_date[0]) != 4 or len(full_date[1]) > 2 or int(full_date[1]) > 12:
            raise TypeError('Некоректный ввод даты! Формат даты должен быть YYYY-MM')
        return {"year": int(full_date[0]), "month": int(full_date[1])}

    @property
    def part_date(self):
        """Возвращает дату последнего дня из рассматриваемого периода"""
        year = self.year + 1 if self.month == 12 else self.year
        month = self.month + 1 if self.month < 12 else 1
        return date(year, month, 1) - timedelta(days=1)

    @property
    def start_period(self):
        """Возвращает начальную дату рассматриваемого периода"""
        # year = self.year - 1 if self.month < 3 else self.year
        # month = 12 if self.month == 2 else (self.month - 2) % 12
        # return date(year, month, 1)
        return date(self.year, self.month, 1)


schema_articles = StructType(fields=[
    StructField("article_id", StringType()),
    StructField("product_code", IntegerType()),
    StructField("prod_name", StringType()),
    StructField("product_type_no", IntegerType()),
    StructField("product_type_name", StringType()),
    StructField("product_group_name", StringType()),
    StructField("graphical_appearance_no", IntegerType()),
    StructField("graphical_appearance_name", StringType()),
    StructField("colour_group_code", IntegerType()),
    StructField("colour_group_name", StringType()),
    StructField("perceived_colour_value_id", IntegerType()),
    StructField("perceived_colour_value_name", StringType()),
    StructField("perceived_colour_master_id", IntegerType()),
    StructField("perceived_colour_master_name", StringType()),
    StructField("department_no", IntegerType()),
    StructField("department_name", StringType()),
    StructField("index_code", StringType()),
    StructField("index_name", StringType()),
    StructField("index_group_no", IntegerType()),
    StructField("index_group_name", StringType()),
    StructField("section_no", IntegerType()),
    StructField("section_name", StringType()),
    StructField("garment_group_no", IntegerType()),
    StructField("garment_group_name", StringType()),
    StructField("text", StringType())
])


schema_customers = StructType(fields=[
    StructField("customer_id", StringType()),
    StructField("FN", FloatType()),
    StructField("Active", FloatType()),
    StructField("club_member_status", StringType()),
    StructField("fashion_news_frequency", StringType()),
    StructField("age", IntegerType()),
    StructField("postal_code", StringType())
])


schema_transactions_train = StructType(fields=[
    StructField("t_dat", DateType()),
    StructField("customer_id", StringType()),
    StructField("article_id", StringType()),
    StructField("price", DoubleType()),
    StructField("sales_channel_id", StringType())
])


def timer(fn):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        print(f"\nНачало выполнения!\n{start_time}\n")
        fn(*args, **kwargs)
        finish_time = datetime.now()
        print(f"\nЗавершено!\n{finish_time}, (затраченное время - {finish_time - start_time})")
    return wrapper


@timer
def main():
    user_date = sys.argv[sys.argv.index("part_date") + 1]
    ex_date = DatesUsed(**DatesUsed.get_year_and_month(user_date))
    print(f"Выбранный период: {ex_date.start_period} -- {ex_date.part_date}")
    spark = SparkSession.builder.master("local").getOrCreate()
    print(f"Начато считывание файлов...", ending="")
    articlesDf = spark.read.csv(
        "articles.csv",
        header=True,
        schema=schema_articles
    )

    customersDf = spark.read.csv(
        "customers.csv",
        header=True,
        schema=schema_customers
    )

    transactionsDf = spark.read.csv(
        "transactions_train.csv",
        header=True,
        schema=schema_transactions_train
    )

    window_most_exp_art = (
        Window
        .partitionBy("customer_id")
        .orderBy(desc("price"), "t_dat")
    )
    print("OK")
    print("Начата обработка...", ending="")
    result_Df = (
        transactionsDf
        .filter(transactionsDf.t_dat.between(ex_date.start_period, ex_date.part_date))
        .join(articlesDf.select("article_id", "product_group_name"), "article_id", "left")
        .withColumn("most_exp_id", first("article_id").over(window_most_exp_art)) 
        .groupBy("customer_id")
        .agg(
            sum("price").alias("transaction_amount"),
            first("most_exp_id").alias("most_exp_article_id"),
            count("*").alias("number_of_articles"),
            countDistinct("product_group_name").alias("number_of_product_groups")
        )
        .join(customersDf.select("customer_id", "age"), "customer_id", "left")
        .withColumn(
            "customer_group_by_age",
            when(customersDf["age"] < 23, "S")
            .when(customersDf["age"] > 59, "R")
            .otherwise("A")
        )
        .withColumn("part_date", lit(ex_date.part_date))
        .select("part_date", "customer_id", "customer_group_by_age", "transaction_amount"
                , "most_exp_article_id", "number_of_articles", "number_of_product_groups")
    )
    print("OK")
    result_Df.show()

    save_folder = "result"
    print(f"Начато сохрание результатов в папку {save_folder} ...")
    (
        result_Df
        .repartition(1)
        .write
        .format("csv")
        .mode("overwrite")
        .option("header", "true")
        .save(save_folder)
    )
    print("Сохранено успешно!")


if __name__ == "__main__":
        main()
