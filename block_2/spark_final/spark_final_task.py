#!/usr/bin/env python
# coding: utf-8


import sys
import json
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import when, sum, count, desc, first, countDistinct, lit, udf, col
from datetime import datetime, date

from pyspark.sql.types import StructType, DoubleType

from schemes_and_date import schemes, date_used


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
                 currency: str,
                 loyality_level: str | int
                ):
        self.save_folder = save_folder
        self.articles_df = articles_df
        self.customers_df = customers_df
        self.transactions_df = transactions_df
        self.base_period = base_period
        self.currency = currency
        self.loyality_level = int(loyality_level)

    @property
    def name_sub_folder(self) -> str:
        """Возвращает строку в формате ГОД-МЕСЯЦ-ВАЛЮТА исходя из текущих показателей"""
        return f"{self.base_period.year}-{self.base_period.month}-{self.currency}"
    
    @property
    def get_csv_path(self) -> str:
        """Возвращает путь csv файла по текущим настройкам"""
        search_csv = [i.path for i in os.scandir(f"{self.save_folder}/{self.name_sub_folder}") if i.name[-4:] == ".csv"]
        if search_csv:
            csv_path = search_csv[0]
            return csv_path
        else:
            raise KeyError(f"Нет csv файла в директории - {self.save_folder}/{self.name_sub_folder}") 
                
    @staticmethod
    def read_csv_to_df(spark_session: "SparkSession",
                       file_path: str,
                       schema: "StructType",
                       header: bool = True,
                       **kwargs) -> "DataFrame":
        """Считывает выбранный csv файл по предоставленной схеме и возвращает на выходе DataFrame"""
        return spark_session.read.csv(file_path, schema=schema, header=header, **kwargs)

    def save_df_to_csv(self, result_df: "DataFrame", name_sub_folder: str = None) -> str:
        """Сохраняет DataFrame в указаную подпапку. На выходе даёт строку с относительным путём до папки."""
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
        return f"{self.save_folder}/{name_sub_folder}"

    @staticmethod
    @udf(returnType=DoubleType())
    def price_by_currency(price, currency, current_exchange_rate, dm_currency):
        """Фнукция для конвертации цены в выбранную валюту"""
        if currency == dm_currency:
            return price
        else:
            dict_exchange_rate = json.loads(current_exchange_rate.replace("'",'"'))
            for i in dict_exchange_rate:
                if i.upper() == dm_currency:
                    return price * dict_exchange_rate[i]
            raise KeyError("Нет курса валюты в списке") 

    @staticmethod
    def get_ordered_name_col(initial_df: "DataFrame", *additional_fields) -> list:
        """Возвращает список полей в исходном порядке"""
        ordered_name_col = initial_df.columns + [i for i in additional_fields if i not in initial_df.columns]
        return ordered_name_col
    
    def get_result_df(self,
                      dm_currency: str = None,
                      start_date: date = None,
                      finish_date: date = None,
                      ) -> "DataFrame":
        """Возвращает базовый результирующий DataFrame исходя из периода и валюты""" 
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
            .select("part_date", "customer_id", "customer_group_by_age", "transaction_amount", "dm_currency",
                    "most_exp_article_id", "number_of_articles", "number_of_product_groups")
        )
    
    def get_loyality_df(self, base_result_df: "DataFrame", loyality_level: int, previous_result_df: "DataFrame" = None) -> "DataFrame":
        """Дополняет или пересчитывает в DataFrame характеристику лояльности клиента"""
        if previous_result_df is None:
            if "loyal_months_nr" in base_result_df.columns:
                loyality_df = base_result_df.drop("customer_loyality").withColumn("customer_loyality", lit(1))
            else:
                loyality_df = base_result_df.withColumns({"loyal_months_nr": lit(1), "customer_loyality": lit(1)})
        else:
            first_step_df = (
                base_result_df
                .drop("loyal_months_nr", "customer_loyality")
                .join(previous_result_df.select("customer_id", col("loyal_months_nr").alias("previous_loyal_months_nr")), "customer_id", "left")
            )
            second_step_df = (
                first_step_df
                .withColumn(
                    "loyal_months_nr",
                    when(first_step_df.previous_loyal_months_nr.isNull(), lit(1))
                    .otherwise(lit(first_step_df.previous_loyal_months_nr + 1))
                )
                .drop("previous_loyal_months_nr")
            )
            loyality_df = (
                second_step_df
                .withColumn(
                    "customer_loyality", 
                    when(second_step_df.loyal_months_nr >= loyality_level, lit(1))
                    .otherwise(lit(0))
                )
            )
        return loyality_df.select(*self.get_ordered_name_col(base_result_df, 'loyal_months_nr', 'customer_loyality'))

    def get_offer_df(self, loyality_result_df: "DataFrame") -> "DataFrame":
        """Дополняет DataFrame характеристикой 'offer' возможного предложения супер акции для пользователя.
        Предварительно нужно обработать DataFrame с помощью метода get_loyality_df"""
        offer_df = (
            loyality_result_df
            .drop("offer")
            .join(self.customers_df.alias("club_and_news").select("customer_id", "club_member_status", "fashion_news_frequency"),
                  "customer_id", "left")
            .withColumn(
                "offer",
                when((loyality_result_df.customer_loyality == 1)
                     & (col("club_and_news.club_member_status") == "ACTIVE")
                     & (col("club_and_news.fashion_news_frequency") == "Regularly"), lit(1))
                .otherwise(lit(0))
            )
        )
        return offer_df.select(*self.get_ordered_name_col(loyality_result_df, 'offer'))

    def get_most_freq_product_df(self, base_result_df: "DataFrame") -> "DataFrame":
        """Дополняет DataFrame характеристикой 'most_freq_product_group_name' группы товаров чаще встречающаеся в покупках за месяц"""
        start_date = self.base_period.start_period
        finish_date = self.base_period.part_date
        window_freq_product_group = Window.partitionBy("customer_id").orderBy(desc("count"))
        customer_product_group_df = (
            self.transactions_df
            .select("customer_id", "article_id")
            .filter(self.transactions_df.t_dat.between(start_date, finish_date))
            .join(self.articles_df.select("article_id", "product_group_name"), "article_id", "left")
            .groupBy(["customer_id", "product_group_name"])
            .count()
            .withColumn("most_freq_product", first("product_group_name").over(window_freq_product_group))
            .groupBy("customer_id")
            .agg(first("most_freq_product").alias("most_freq_product_group_name"))
        )
        most_freq_product_df = base_result_df.join(customer_product_group_df, "customer_id", "left")
        return most_freq_product_df.select(*self.get_ordered_name_col(base_result_df, 'most_freq_product_group_name'))


def get_start_parameters(*args):
    """Забирает выбранные параметры из условий запуска"""
    parameters = dict()
    try:
        for i in args:
            parameters[i] = sys.argv[sys.argv.index(i) + 1]
    except (ValueError, IndexError):
        raise KeyError(f"Нехватает параметра(ов) и/или их значений: {args}")
    return parameters

def checking_parameters(start_parameters: dict):
    """Проверяет параметры на соответствие требованиям"""
    TYPES_CURRENCIES = ("USD", "EUR", "BYN", "PLN")
    if start_parameters["dm_currency"] not in TYPES_CURRENCIES:
        raise KeyError(f"Указан недопустимый тип валюты. Выберете из предложенных: {TYPES_CURRENCIES}")
    if not start_parameters["loyality_level"].isdecimal():
        raise KeyError(f"В уровне лояльности указано не число, а -- {start_parameters['loyality_level']}")


@timer
def main():
    spark = SparkSession.builder.master("local").getOrCreate()
    try:
        start_parameters = get_start_parameters("part_date", "dm_currency", "loyality_level")
        checking_parameters(start_parameters)
        period = date_used.DatesUsed(**date_used.DatesUsed.get_year_and_month(start_parameters["part_date"]))
        print(f"Выбранный период: {period.start_period} -- {period.part_date}")

        print(f"Начато считывание исходных файлов...", end="")
        df_manager = DataFrameManager(
            save_folder = "result_final_task_spark",
            articles_df = DataFrameManager.read_csv_to_df(spark, "articles.csv", schema=schemes.schema_articles),
            customers_df = DataFrameManager.read_csv_to_df(spark, "customers.csv", schema=schemes.schema_customers),
            transactions_df = DataFrameManager.read_csv_to_df(spark, "transactions_train_with_currency.csv", schema=schemes.schema_transactions_train_currency),
            base_period = period,
            currency = start_parameters["dm_currency"],
            loyality_level = start_parameters["loyality_level"]
        )
        print("OK")

        if not os.path.isdir(df_manager.save_folder):
             os.mkdir(df_manager.save_folder)
        current_dirs = [i.name for i in os.scandir(df_manager.save_folder) if i.is_dir()]
        df_manager.base_period.month_offset(1-df_manager.loyality_level)
        previous_loyal_months_df = None
        for i in range(df_manager.loyality_level):
            print(f"Этап {i+1}. ({datetime.now()})")
            if df_manager.name_sub_folder in current_dirs:
                print(f"Базовый расчёт для параметров {df_manager.name_sub_folder} уже существует.")
                result_df = DataFrameManager.read_csv_to_df(spark, df_manager.get_csv_path, schema=schemes.schema_result)
                print(f"Прочитан файл - {df_manager.get_csv_path}")
            else:
                print(f"Начата обработка задачи для параметров {df_manager.name_sub_folder}.")
                result_df = df_manager.get_most_freq_product_df(df_manager.get_result_df())
                
            loyality_df = df_manager.get_loyality_df(base_result_df = result_df, loyality_level = i + 1, previous_result_df = previous_loyal_months_df)
            offer_df = df_manager.get_offer_df(loyality_df)
            df_manager.save_df_to_csv(offer_df)
            print(f"Сохранено в {df_manager.get_csv_path}")
            print(f"Этап {i+1} Завершён. ({datetime.now()})")
            previous_loyal_months_df = DataFrameManager.read_csv_to_df(spark, df_manager.get_csv_path, schema=schemes.schema_result).select("customer_id", "loyal_months_nr")
            df_manager.base_period.month_offset(1)
            
    except Exception as ex:
        print(f"\nПроизошла ошибка: {ex}")
        raise ex
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
