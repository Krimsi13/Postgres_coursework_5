from typing import Any

import requests


def get_data_from_hh(employer_ids: list[str]) -> list[dict[str, Any]]:
    """Получение данных о работодателях и вакансиях"""

    data = []
    for employer_id in employer_ids:
        response_employer = requests.get('https://api.hh.ru/employers/' + employer_id)
        employer_data = response_employer.json()

        vacancy_data = []
        response_vacancy = requests.get('https://api.hh.ru/vacancies?employer_id=' + employer_id)
        response_text_vac = response_vacancy.json()

        vacancy_data.extend(response_text_vac['items'])

        data.append({
            'employers': employer_data,
            'vacancies': vacancy_data
        })

    return data
