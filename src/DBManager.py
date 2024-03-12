from typing import Any

import psycopg2


class DBManager:

    def __init__(self, params: dict):
        self.params = params

    def create_data_base(self, database_name: str) -> None:
        """Создание базы данных"""

        conn = psycopg2.connect(dbname='postgres', **self.params)
        conn.autocommit = True

        cur = conn.cursor()

        cur.execute(f'DROP DATABASE {database_name}')
        cur.execute(f'CREATE DATABASE {database_name}')

        conn.close()

    def create_tables(self, database_name: str) -> None:
        """Создание таблиц"""

        with psycopg2.connect(dbname=database_name, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE employers (
                                employer_id SERIAL PRIMARY KEY,
                                company_name VARCHAR(500) NOT NULL,
                                open_vacancies INTEGER,
                                employer_url TEXT,
                                description TEXT
                                )
                            """)

            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE vacancies (
                        vacancy_id SERIAL PRIMARY KEY,
                        employer_id INT REFERENCES employers(employer_id),
                        vacancy_name VARCHAR(500) NOT NULL,
                        salary_from INTEGER,
                        salary_to INTEGER,
                        vacancy_url TEXT)
                """)

        conn.close()

    def save_data_to_database(self, data: list[dict[str, Any]], database_name: str) -> None:
        """Сохранение данных в базу данных."""

        conn = psycopg2.connect(dbname=database_name, **self.params)

        with conn.cursor() as cur:
            for text in data:
                employer_data = text['employers']

                cur.execute(
                    """
                    INSERT INTO employers (company_name, open_vacancies, employer_url, description)
                    VALUES (%s, %s, %s, %s)
                    RETURNING employer_id
                    """,
                    (employer_data['name'], employer_data['open_vacancies'], employer_data['alternate_url'],
                     employer_data['description']))

                employer_id = cur.fetchone()[0]

                vacancies_data = text['vacancies']
                for vacancy in vacancies_data:
                    if vacancy['salary'] is None:
                        continue

                    elif vacancy['salary']['from'] is None:
                        cur.execute(
                            """
                            INSERT INTO vacancies (employer_id, vacancy_name, salary_from, salary_to, vacancy_url)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (employer_id, vacancy['name'], 0, vacancy['salary']['to'],
                             vacancy['alternate_url'])
                        )

                    elif vacancy['salary']['to'] is None:
                        cur.execute(
                            """
                            INSERT INTO vacancies (employer_id, vacancy_name, salary_from, salary_to, vacancy_url)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (employer_id, vacancy['name'], vacancy['salary']['from'], vacancy['salary']['from'],
                             vacancy['alternate_url'])
                        )

                    else:
                        cur.execute(
                            """
                            INSERT INTO vacancies (employer_id, vacancy_name, salary_from, salary_to, vacancy_url)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (employer_id, vacancy['name'], vacancy['salary']['from'], vacancy['salary']['to'],
                             vacancy['alternate_url']))

        conn.commit()
        conn.close()

    def get_companies_and_vacancies_count(self, database_name: str) -> list:
        """Получение списка всех компаний и количества вакансий у каждой компании"""

        with psycopg2.connect(dbname=database_name, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT company_name, open_vacancies FROM employers")

                data = cur.fetchall()

                data_dict = [{"company_name": d[0], "open_vacancies": d[1]} for d in data]

        conn.close()

        return data_dict

    def get_all_vacancies(self, database_name: str) -> list:
        """Получение списка всех вакансий """

        with psycopg2.connect(dbname=database_name, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                            SELECT employers.company_name, vacancies.vacancy_name,
                            vacancies.salary_from, vacancies.salary_to, vacancies.vacancy_url
                            FROM vacancies
                            JOIN employers USING(employer_id)
                            """)

                data = cur.fetchall()

                data_dict = [{"company_name": d[0],
                              "vacancy_name": d[1],
                              "salary_from": d[2],
                              "salary_to": d[3],
                              "vacancy_url": d[4]} for d in data]

        conn.close()

        return data_dict

    def get_avg_salary(self, database_name: str) -> int:
        """Получение средней зарплаты по вакансиям"""

        with psycopg2.connect(dbname=database_name, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                            SELECT cast(AVG((salary_from + salary_to)/2) as integer)
                            FROM vacancies
                            """)

                avg_salary = cur.fetchone()[0]

        conn.close()

        return avg_salary

    def get_vacancies_with_higher_salary(self, database_name: str) -> list:
        """Получение списка всех вакансий, у которых зарплата выше средней по всем вакансиям"""
        with psycopg2.connect(dbname=database_name, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                            SELECT employers.company_name, vacancies.vacancy_name,
                            vacancies.salary_from, vacancies.salary_to, vacancies.vacancy_url FROM vacancies
                            JOIN employers USING(employer_id)
                            WHERE (salary_from + salary_to)/2 > (SELECT AVG((salary_from + salary_to)/2)
                            FROM vacancies)
                            """)

                data = cur.fetchall()

                data_dict = [{"company_name": d[0],
                              "vacancy_name": d[1],
                              "salary_from": d[2],
                              "salary_to": d[3],
                              "vacancy_url": d[4]} for d in data]

        conn.close()

        return data_dict

    def get_vacancies_with_keyword(self, database_name: str, keyword: str) -> list:
        """Получение списка всех вакансий, в названии которых содержится переданное в метод слово, например менеджер"""
        with psycopg2.connect(dbname=database_name, **self.params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                            SELECT employers.company_name, vacancies.vacancy_name,
                            vacancies.salary_from, vacancies.salary_to, vacancies.vacancy_url FROM vacancies
                            JOIN employers USING(employer_id)
                            WHERE vacancy_name LIKE '%{keyword}%' OR vacancy_name LIKE '%{keyword.title()}%'
                            """)

                data = cur.fetchall()

                data_dict = [{"company_name": d[0],
                              "vacancy_name": d[1],
                              "salary_from": d[2],
                              "salary_to": d[3],
                              "vacancy_url": d[4]} for d in data]

        conn.close()

        return data_dict
