from config import config
from src.DBManager import DBManager
from src.parser import get_data_from_hh


def main():
    employer_ids = [
        '5920492',  # DNS Головной офис
        '9311920',  # DNS Технологии
        '2347518',  # ДНС ЛГ
        '6075761',  # Днс Море
        '2116988',  # ДНС ПродИмпорт?
        '10699153',  # ДНС ЭЛЕКТРОНИКА
        '5240131',  # Сеть магазинов цифровой и бытовой техники ДНС КАЗАХСТАН
        '9694561',  # Яндекс.Еда
        '5008932',  # Яндекс Практикум
        '9439962',  # Яндекс.Такси
    ]

    params = config()
    dbm = DBManager(params)

    data = get_data_from_hh(employer_ids)

    dbm.create_data_base('postgres_coursework_5')
    dbm.create_tables('postgres_coursework_5')

    dbm.save_data_to_database(data, 'postgres_coursework_5')

    print(dbm.get_companies_and_vacancies_count('postgres_coursework_5'))
    print(dbm.get_all_vacancies('postgres_coursework_5'))
    print(dbm.get_avg_salary('postgres_coursework_5'))
    print(dbm.get_vacancies_with_higher_salary('postgres_coursework_5'))

    keyword = input("Введите слово для поиска по названию: ").lower()
    print(dbm.get_vacancies_with_keyword('postgres_coursework_5', keyword))


if __name__ == '__main__':
    main()