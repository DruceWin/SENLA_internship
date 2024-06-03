import sys
import json
import os
import pandas as pd
from datetime import datetime, date

from date_used import DatesUsed


def timer(fn):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        print(f"\nНачало выполнения!\n{start_time}\n")
        fn(*args, **kwargs)
        finish_time = datetime.now()
        print(f"\nЗавершено!\n{finish_time}, (затраченное время - {finish_time - start_time})")

    return wrapper


class DataFrameManager:
    def __init__(
            self,
            save_folder: str,
            articles_df: pd.DataFrame,
            customers_df: pd.DataFrame,
            transactions_df: pd.DataFrame,
            base_period: "DatesUsed",
            currency: str,
            loyalty_level: str | int
    ):
        self.save_folder = save_folder
        self.articles_df = articles_df
        self.customers_df = customers_df
        self.transactions_df = transactions_df
        self.base_period = base_period
        self.currency = currency
        self.loyalty_level = int(loyalty_level)

    @property
    def name_sub_folder(self) -> str:
        """Возвращает строку в формате ГОД-МЕСЯЦ-ВАЛЮТА исходя из текущих показателей"""
        return f"{self.base_period.year}-{self.base_period.month}-{self.currency}"

    @property
    def get_csv_path(self) -> str:
        """Возвращает путь csv файла по текущим настройкам"""
        search_csv = [i.path for i in os.scandir(f"{self.save_folder}/{self.name_sub_folder}") if
                      i.name.endswith(".csv")]
        if search_csv:
            return search_csv[0]
        else:
            raise KeyError(f"Нет csv файла в директории - {self.save_folder}/{self.name_sub_folder}")

    @staticmethod
    def read_csv_to_df(file_path: str, **kwargs) -> pd.DataFrame:
        """Считывает выбранный csv файл и возвращает на выходе DataFrame"""
        return pd.read_csv(file_path, **kwargs)

    def save_df_to_csv(self, result_df: pd.DataFrame, name_sub_folder: str = None) -> str:
        """Сохраняет DataFrame в указанную подпапку. На выходе даёт строку с относительным путём до папки."""
        if not name_sub_folder:
            name_sub_folder = self.name_sub_folder
        os.makedirs(f"{self.save_folder}/{name_sub_folder}", exist_ok=True)
        result_df.to_csv(f"{self.save_folder}/{name_sub_folder}/result.csv", index=False)
        return f"{self.save_folder}/{name_sub_folder}"

    @staticmethod
    def price_by_currency(price, currency, current_exchange_rate, dm_currency):
        """Функция для конвертации цены в выбранную валюту"""
        if currency == dm_currency:
            return price
        else:
            dict_exchange_rate = json.loads(current_exchange_rate.replace("'", '"'))
            return price * dict_exchange_rate.get(dm_currency, 1.0)

    @staticmethod
    def get_ordered_name_col(initial_df: pd.DataFrame, *additional_fields) -> list:
        """Возвращает список полей в исходном порядке"""
        return list(initial_df.columns) + [i for i in additional_fields if i not in initial_df.columns]

    def get_result_df(
            self,
            dm_currency: str = None,
            start_date: date = None,
            finish_date: date = None,
    ) -> pd.DataFrame:
        """Возвращает базовый результирующий DataFrame исходя из периода и валюты"""
        if not dm_currency:
            dm_currency = self.currency
        if not start_date:
            start_date = self.base_period.start_period
        if not finish_date:
            finish_date = self.base_period.part_date

        filtered_transactions_df = self.transactions_df[
            (pd.to_datetime(self.transactions_df['t_dat']) >= pd.to_datetime(start_date)) &
            (pd.to_datetime(self.transactions_df['t_dat']) <= pd.to_datetime(finish_date))
            ]

        filtered_transactions_df['price'] = filtered_transactions_df.apply(
            lambda row: self.price_by_currency(
                row['price'],
                row['currency'],
                row['current_exchange_rate'],
                dm_currency
            ),
            axis=1
        )

        aggregated_transactions = filtered_transactions_df.merge(
            self.articles_df['article_id', 'product_group_name'],
            on='article_id',
            how='left'
        ).groupby(['customer_id', 'article_id']).agg({
            'price': 'max',
            't_dat': 'first'
        }).reset_index()

        most_exp_art_df = aggregated_transactions.loc[
            aggregated_transactions.groupby('customer_id')['price'].idxmax()
        ]

        customer_group_data = filtered_transactions_df.groupby('customer_id').agg(
            transaction_amount=('price', 'sum'),
            most_exp_article_id=('article_id', 'first'),
            number_of_articles=('article_id', 'count'),
            number_of_product_groups=('product_group_name', pd.Series.nunique)
        ).reset_index()

        enriched_customer_data = customer_group_data.merge(
            self.customers_df['customer_id', 'age'],
            on='customer_id',
            how='left'
        )

        enriched_customer_data['customer_group_by_age'] = enriched_customer_data['age'].apply(
            lambda x: 'S' if x < 23 else ('R' if x > 59 else 'A')
        )

        enriched_customer_data['part_date'] = finish_date
        enriched_customer_data['dm_currency'] = dm_currency

        result_df = enriched_customer_data[
            'part_date', 'customer_id', 'customer_group_by_age', 'transaction_amount', 'dm_currency',
            'most_exp_article_id', 'number_of_articles', 'number_of_product_groups'
        ]

        return result_df

    @staticmethod
    def get_loyalty_df(base_result_df: pd.DataFrame, loyalty_level: int,
                       previous_result_df: pd.DataFrame = None) -> pd.DataFrame:
        """Дополняет или пересчитывает в DataFrame характеристику лояльности клиента"""
        if previous_result_df is None:
            base_result_df['loyal_months_nr'] = 1
            base_result_df['customer_loyalty'] = 1
            loyalty_df = base_result_df
        else:
            merged_df = base_result_df.merge(
                previous_result_df['customer_id', 'loyal_months_nr'],
                on='customer_id',
                how='left',
                suffixes=('', '_previous')
            )
            merged_df['loyal_months_nr'] = merged_df['loyal_months_nr_previous'].fillna(0) + 1
            merged_df['customer_loyalty'] = (merged_df['loyal_months_nr'] >= loyalty_level).astype(int)
            loyalty_df = merged_df.drop(columns=['loyal_months_nr_previous'])

        return loyalty_df

    def get_offer_df(self, loyalty_result_df: pd.DataFrame) -> pd.DataFrame:
        """Дополняет DataFrame характеристикой 'offer' возможного предложения супер акции для пользователя."""
        enriched_offer_df = loyalty_result_df.merge(
            self.customers_df['customer_id', 'club_member_status', 'fashion_news_frequency'],
            on='customer_id',
            how='left'
        )

        enriched_offer_df['offer'] = (
                (enriched_offer_df['customer_loyalty'] == 1) &
                (enriched_offer_df['club_member_status'] == 'ACTIVE') &
                (enriched_offer_df['fashion_news_frequency'] == 'Regularly')
        ).astype(int)

        return enriched_offer_df[
            *loyalty_result_df.columns.tolist(),
            'offer'
        ]

    def get_most_freq_product_df(self, base_result_df: pd.DataFrame) -> pd.DataFrame:
        """
        Дополняет DataFrame характеристикой 'most_freq_product_group_name' 
        группы товаров чаще встречающейся в покупках за месяц
        """
        start_date = self.base_period.start_period
        finish_date = self.base_period.part_date

        customer_product_group_df = (
            self.transactions_df[
                (pd.to_datetime(self.transactions_df['t_dat']) >= pd.to_datetime(start_date)) &
                (pd.to_datetime(self.transactions_df['t_dat']) <= pd.to_datetime(finish_date))
                ]
            .merge(self.articles_df['article_id', 'product_group_name'], on='article_id', how='left')
            .groupby(['customer_id', 'product_group_name']).size()
            .reset_index(name='count')
        )

        most_freq_product_df = (
            customer_product_group_df.loc[customer_product_group_df.groupby('customer_id')['count'].idxmax()]
            ['customer_id', 'product_group_name'].rename(
                columns={'product_group_name': 'most_freq_product_group_name'})
        )

        result_df = base_result_df.merge(most_freq_product_df, on='customer_id', how='left')

        return result_df[
            *base_result_df.columns.tolist(),
            'most_freq_product_group_name'
        ]


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
    if not start_parameters["loyalty_level"].isdecimal():
        raise KeyError(f"В уровне лояльности указано не число, а -- {start_parameters['loyalty_level']}")


@timer
def main():
    try:
        # start_parameters = get_start_parameters("part_date", "dm_currency", "loyalty_level")
        start_parameters = dict()
        start_parameters["part_date"] = "2018-09"
        start_parameters["dm_currency"] = "USD"
        start_parameters["loyalty_level"] = "1"
        checking_parameters(start_parameters)
        period = DatesUsed(**DatesUsed.get_year_and_month(start_parameters["part_date"]))
        print(f"Выбранный период: {period.start_period} -- {period.part_date}")

        print(f"Начато считывание исходных файлов...", end="")
        df_manager = DataFrameManager(
            save_folder="result_final_task_pandas",
            articles_df=pd.read_csv("articles.csv"),
            customers_df=pd.read_csv("customers.csv"),
            transactions_df=pd.read_csv("transactions_train_with_currency.csv"),
            base_period=period,
            currency=start_parameters["dm_currency"],
            loyalty_level=start_parameters["loyalty_level"]
        )
        print("OK")

        if not os.path.isdir(df_manager.save_folder):
            os.mkdir(df_manager.save_folder)
        current_dirs = [i.name for i in os.scandir(df_manager.save_folder) if i.is_dir()]
        df_manager.base_period.month_offset(1 - df_manager.loyalty_level)
        previous_loyal_months_df = None
        for i in range(df_manager.loyalty_level):
            print(f"Этап {i + 1}. ({datetime.now()})")
            if df_manager.name_sub_folder in current_dirs:
                print(f"Базовый расчёт для параметров {df_manager.name_sub_folder} уже существует.")
                result_df = DataFrameManager.read_csv_to_df(df_manager.get_csv_path)
                print(f"Прочитан файл - {df_manager.get_csv_path}")
            else:
                print(f"Начата обработка задачи для параметров {df_manager.name_sub_folder}.")
                result_df = df_manager.get_most_freq_product_df(df_manager.get_result_df())

            loyalty_df = df_manager.get_loyalty_df(base_result_df=result_df, loyalty_level=i + 1,
                                                   previous_result_df=previous_loyal_months_df)
            offer_df = df_manager.get_offer_df(loyalty_df)
            df_manager.save_df_to_csv(offer_df)
            print(f"Сохранено в {df_manager.get_csv_path}")
            print(f"Этап {i + 1} Завершён. ({datetime.now()})")
            previous_loyal_months_df = DataFrameManager.read_csv_to_df(df_manager.get_csv_path).filter(
                ['customer_id', 'loyal_months_nr']
            )
            df_manager.base_period.month_offset(1)

    except Exception as ex:
        print(f"\nПроизошла ошибка: {ex}")
        raise ex


if __name__ == "__main__":
    main()
