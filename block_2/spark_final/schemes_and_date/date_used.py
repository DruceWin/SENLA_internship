from datetime import date, timedelta


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
            raise TypeError('Некорректный ввод даты! Формат даты должен быть YYYY-MM')
        return {"year": int(full_date[0]), "month": int(full_date[1])}

    @property
    def part_date(self) -> date:
        """Возвращает дату последнего дня из рассматриваемого периода"""
        year = self.year + 1 if self.month == 12 else self.year
        month = self.month + 1 if self.month < 12 else 1
        return date(year, month, 1) - timedelta(days=1)

    @property
    def start_period(self) -> date:
        """Возвращает начальную дату рассматриваемого периода"""
        return date(self.year, self.month, 1)

    def month_offset(self, month_dif: int) -> date:
        """Менят исходные параметры года и месяца исходя требуемой разницы месяцов (month_dif)"""
        months = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        self.year = self.year + (self.month + month_dif - 1) // 12
        self.month = months[(self.month - 1 + month_dif) % (12 if month_dif >= 0 else -12)]
