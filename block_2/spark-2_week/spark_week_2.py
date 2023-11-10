#!/usr/bin/env python
# coding: utf-8

import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import count, desc, first, split, lower, explode, regexp_replace, concat_ws, col, trim
from datetime import datetime


SAVE_FOLDER = "result_week_2"


def timer(fn):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        print(f"\nНачало выполнения!\n{start_time}\n")
        fn(*args, **kwargs)
        finish_time = datetime.now()
        print(f"\nЗавершено!\n{finish_time}, (затраченное время - {finish_time - start_time})")
    return wrapper


def read_txts_to_df(spark_session: "SparkSession",
                    dir_name: str,
                    **kwargs) -> "DataFrame":
    """Считывает txt файлы из выбранного репозитория и возвращает на выходе DataFrame"""   
    schema_words = StructType(fields=[
        StructField("rows", StringType())
    ])
    
    base_df = spark_session.createDataFrame([], schema_words)
    [base_df := base_df.union(spark_session.read.text(i.path, **kwargs)) for i in os.scandir(dir_name) if i.is_file()]
    return base_df


def get_word_df(rows_df: "DataFrame") -> "DataFrame":
    return (
        rows_df
        .select(explode(split(lower(trim(regexp_replace(
            regexp_replace(rows_df.rows, "[^a-zA-Z'\s]", ""), "\s{2,}", " "))), "\s+")).alias("word"))
        .filter(col("word").like("_%"))
    )


def get_df_task_1(word_df: "DataFrame") -> "DataFrame":
    return word_df.groupBy("word").agg(count("*").alias("count")).sort(desc("count"))


def get_df_task_2(word_df: "DataFrame") -> "DataFrame":
    words = word_df.withColumn("next_word", first("word").over(Window.rowsBetween(1, 1)))
    return (
        words
        .select(concat_ws(' ', words.word, words.next_word).alias("word"))
        .groupBy("concat_word").agg(count("*").alias("count"))
        .sort(desc("count"))
        .filter(col("concat_word").like("%_ _%"))
    )


def save_df_to_csv(result_df: "DataFrame", name_sub_folder: str):
    (
        result_df
        .repartition(1)
        .write
        .format("csv")
        .mode("overwrite")
        .option("header", "true")
        .save(f"{SAVE_FOLDER}/{name_sub_folder}")
    )
    return f"{SAVE_FOLDER}/{name_sub_folder}"


@timer
def main():
    spark = SparkSession.builder.master("local").getOrCreate()
    try:
        print(f"Начато считывание файлов...", end="")
        rows_df = read_txts_to_df(spark, "txt_files")
        print("OK")

        word_df = get_word_df(rows_df)
        print(f"Общее количество слов - {word_df.count()}")

        print("Начата обработка задачи 1...", end="")
        result_df_1 = get_df_task_1(word_df)
        folder = save_df_to_csv(result_df_1, "task_1")
        print(f"OK \nСохранено в {folder}")
        print(f"Количество уникальных слов - {result_df_1.count()}")
        
        print("Начата обработка задачи 2...", end="")
        result_df_2 = get_df_task_2(word_df)
        folder = save_df_to_csv(result_df_2, "task_2")
        print(f"OK \nСохранено в {folder}")
        print(f"Количество уникальных биграмм - {result_df_2.count()}")
    except Exception as ex:
        print(f"Произошла ошибка: {ex}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
