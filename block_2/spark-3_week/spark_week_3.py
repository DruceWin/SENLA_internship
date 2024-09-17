#!/usr/bin/env python
# coding: utf-8
import sys
import json
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when, sum, count, desc, first, countDistinct, lit, udf
from datetime import datetime, date

from pyspark.sql.types import StructType, DoubleType

from schemes_and_date import schemes, date_used

TYPES_CURRENCIES = ("USD", "EUR", "BYN", "PLN")


def timer(fn):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        print(f"\nНачало выполнения!\n{start_time}\n")
        fn(*args, **kwargs)
        finish_time = datetime.now()
        print(f"\nЗавершено!\n{finish_time}, (затраченное время - {finish_time - start_time})")
    return wrapper


class DataFrameManager:
    def __init__(self, 
                 save_folder: str, 
                 articles_df: "DataFrame", 
                 customers_df: "DataFrame", 
                 transactions_df: "DataFrame",
                 base_period: "DatesUsed",
                 currency: str
                ):
        self.save_folder = save_folder
        self.articles_df = articles_df
        self.customers_df = customers_df
        self.transactions_df = transactions_df
        self.base_period = base_period
        self.currency = currency

    @staticmethod
    def read_csv_to_df(spark_session: "SparkSession",
                       file_name: str,
                       schema: "StructType",
                       header: bool = True,
                       **kwargs) -> "DataFrame":
        """Считывает выбранный csv файл по предоставленной схеме и возвращает на выходе DataFrame"""
        return spark_session.read.csv(file_name, schema=schema, header=header, **kwargs)

    def save_df_to_csv(self, 
                       result_df: "DataFrame",
                       name_sub_folder: str = None
                      ) -> str:
        if not name_sub_folder:
            name_sub_folder = self.name_sub_folder
        (
            result_df
            .repartition(1)
            .write
            .format("csv")
            .mode("overwrite")
            .option("header", "true")
            .save(f"{self.save_folder}/{name_sub_folder}")
        )
        """Сохраняет DataFrame в указаную подпапку"""
        return f"{self.save_folder}/{name_sub_folder}"

    @staticmethod
    @udf(returnType=DoubleType())
    def price_by_currency(price, currency, current_exchange_rate, dm_currency):
        if currency == dm_currency:
            return price
        else:
            dict_exchange_rate = json.loads(current_exchange_rate.replace("'",'"'))
            for i in dict_exchange_rate:
                if i.upper() == dm_currency:
                    return price * dict_exchange_rate[i]
            raise KeyError('Нет курса валюты в списке') 
    
    def get_result_df(self,
                      dm_currency: str = None,
                      start_date: date = None,
                      finish_date: date = None,
                      ) -> "DataFrame":
        """Возвращает результирующий DataFrame исходя из периода и валюты""" 
        if not dm_currency:
            dm_currency = self.currency
        if not start_date:
            start_date = self.base_period.start_period
        if not finish_date:
            finish_date = self.base_period.part_date
        window_most_exp_art = Window.partitionBy("customer_id").orderBy(desc("price"), "t_dat")
        return (
            self.transactions_df
            .filter(self.transactions_df.t_dat.between(start_date, finish_date))
            .withColumn("price", self.price_by_currency(
                "price", "currency", "current_exchange_rate", lit(dm_currency)
            ))
            .join(self.articles_df.select("article_id", "product_group_name"), "article_id", "left")
            .withColumn("most_exp_id", first("article_id").over(window_most_exp_art))
            .groupBy("customer_id")
            .agg(
                sum("price").alias("transaction_amount"),
                first("most_exp_id").alias("most_exp_article_id"),
                count("*").alias("number_of_articles"),
                countDistinct("product_group_name").alias("number_of_product_groups")
            )
            .join(self.customers_df.select("customer_id", "age"), "customer_id", "left")
            .withColumn(
                "customer_group_by_age",
                when(self.customers_df["age"] < 23, "S")
                .when(self.customers_df["age"] > 59, "R")
                .otherwise("A")
            )
            .withColumn("part_date", lit(finish_date))
            .withColumn("dm_currency", lit(dm_currency))
            .select("part_date", "customer_id", "customer_group_by_age", "transaction_amount",
                    "most_exp_article_id", "number_of_articles", "number_of_product_groups", "dm_currency")
        )

    @property
    def name_sub_folder(self) -> str:
        return f"{self.base_period.year}-{self.base_period.month}-{self.currency}"


@timer
def main():
    spark = SparkSession.builder.master("local").getOrCreate()
    try:
        user_date = sys.argv[sys.argv.index("part_date") + 1]
        # user_date = "2018-10"
        dm_currency = sys.argv[sys.argv.index("dm_currency") + 1]
        # dm_currency = "USD"
        if not user_date or not dm_currency:
            raise KeyError('Нехватает параметра part_date и/или dm_currency')
        if dm_currency not in TYPES_CURRENCIES:
            raise KeyError(f'Указан недопустимый тип валюты. Выберете из предложенных: {TYPES_CURRENCIES}')
        period = date_used.DatesUsed(**date_used.DatesUsed.get_year_and_month(user_date))
        print(f"Выбранный период: {period.start_period} -- {period.part_date}")

        print(f"Начато считывание файлов...", end="")
        df_manager = DataFrameManager(
            save_folder = "result_week_3",
            articles_df = DataFrameManager.read_csv_to_df(spark, "articles.csv", schema=schemes.schema_articles),
            customers_df = DataFrameManager.read_csv_to_df(spark, "customers.csv", schema=schemes.schema_customers),
            transactions_df = DataFrameManager.read_csv_to_df(spark, "transactions_train_with_currency.csv", schema=schemes.schema_transactions_train_currency),
            base_period = period,
            currency= dm_currency
        )
        print("OK")

        if not os.path.isdir(df_manager.save_folder):
             os.mkdir(df_manager.save_folder)
        current_dirs = [i.name for i in os.scandir(df_manager.save_folder) if i.is_dir()]
        for i in range(3):
            if df_manager.name_sub_folder in current_dirs:
                print(f"Расчёт для параметров уже существует расчёт {df_manager.name_sub_folder}")
            else:
                print(f"Начата обработка задачи для параметров {df_manager.name_sub_folder} ...", end="")
                result_df = df_manager.get_result_df()
                folder = df_manager.save_df_to_csv(result_df)
                print(f"OK \nСохранено в {folder}")
            df_manager.base_period.month_offset(-1)   
        
    except Exception as ex:
        print(f"\nПроизошла ошибка: {ex}")
        raise ex
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
