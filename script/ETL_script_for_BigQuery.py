
# Author - Muhammad Usman - 09/09/2022

import os
import requests
import pandas as pd
from datetime import datetime, date
from google.oauth2 import service_account
from google.cloud import bigquery


# main request function
def main_request(baseurl, endpoint, headers):
    # using requests to call API data
    r = requests.get(baseurl + endpoint, headers=headers)
    # returning data in json format
    return r.json()


def calculate_age(birthdate):
    born = datetime.strptime(birthdate, '%Y-%m-%dT%H:%M:%S.%fZ')
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


def anonymization_birthdate(birthdate):
    return '{:%Y-01-01}'.format(datetime.strptime(birthdate, '%Y-%m-%dT%H:%M:%S.%fZ'))


def extract_domain(email):
    # slicing domain name using slicing
    res = email[email.index('@') + 1:]
    return str(res)


def google_cloud_authentication():
    dir_name = os.path.dirname(__file__)
    base_filename = 'key'
    filename_suffix = '.json'
    filename = os.path.join(dir_name, base_filename + filename_suffix)
    print(filename)

    svc_acc_cred = service_account.Credentials.from_service_account_file(
        filename=filename,
        # filename=os.environ['GOOGLE_APPLICATION_CREDENTIALS'],
        scopes=['https://www.googleapis.com/auth/cloud-platform'])
    return svc_acc_cred


def load(cleaned_users_data_to_load, cleaned_msg_data_to_load):
    credentials = google_cloud_authentication()
    project_id = 'rational-camera-361514'
    user_table = 'spark_network_challenge.users_table'
    msg_table = 'spark_network_challenge.message_table'

    client = bigquery.Client(credentials=credentials, project=project_id)
    # Load data to Big Query User Tables
    job_config_users_table = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('age', 'INTEGER'),
            bigquery.SchemaField('id', 'INTEGER')
        ])
    cleaned_users_data_to_load = cleaned_users_data_to_load.astype({"id": "int", "age": "int"})
    client.load_table_from_dataframe(cleaned_users_data_to_load, user_table, job_config=job_config_users_table)

    # Load data to Big Query Message Tables
    job_config_msg_table = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('receiverId', 'INTEGER'),
            bigquery.SchemaField('id', 'INTEGER'),
            bigquery.SchemaField('senderId', 'INTEGER')
        ])
    cleaned_msg_data_to_load = cleaned_msg_data_to_load.astype({"receiverId": "int", "id": "int", "senderId": "int"})
    client.load_table_from_dataframe(cleaned_msg_data_to_load, msg_table, job_config=job_config_msg_table)


def extract():
    # Defining headers
    headers = {
        'accept': 'application/json'
    }

    # Defining Urls
    base_url = 'https://619ca0ea68ebaa001753c9b0.mockapi.io/evaluation/dataengineer/jr/v1/'

    # Defining endpoints
    end_point_user = 'users'
    end_point_msg = 'messages'

    # variable calling main function
    users_data = main_request(baseurl=base_url, endpoint=end_point_user, headers=headers)
    msg_data = main_request(baseurl=base_url, endpoint=end_point_msg, headers=headers)

    # creating a dataframe using pandas
    # USER DATA
    # normalize_users_dataframe = pd.json_normalize(users_data)
    normalize_users_dataframe = pd.DataFrame.from_dict(users_data)

    # MESSAGE DATA
    # normalize_msg_dataframe = pd.json_normalize(msg_data)
    normalize_msg_dataframe = pd.DataFrame.from_dict(msg_data)

    return normalize_users_dataframe, normalize_msg_dataframe


def cleaning(users_data, msg_data):
    # Extract Domain name from Emails
    users_data['email'] = users_data['email'].apply(extract_domain)

    # anonymization birthdate and Calculate Age form the BirthData
    users_data['age'] = users_data['birthDate'].apply(calculate_age)
    users_data['birthDate'] = users_data['birthDate'].apply(anonymization_birthdate)

    # Remove all PII ID's
    users_data.drop(
        ['firstName', 'lastName', 'address', 'zipCode'], axis=1,
        inplace=True)

    # Rename Profile Columns
    # users_data.rename(
    #     columns={'profile.gender': 'gender', 'profile.isSmoking': 'isSmoking', 'profile.income': 'income', 'profile.profession': 'profession'},
    #     inplace=True)

    # SUBSCRIPTION DATA
    # subscriptionData = pd.json_normalize(user_data, record_path=['subscription'], meta=['id'])
    # subscriptionData.rename(columns={'id': 'userid'}, inplace=True)

    # MESSAGE DATA
    msg_data.drop(['message'], axis=1, inplace=True)

    return users_data, msg_data


if __name__ == '__main__':
    users_dataframe, msg_dataframe = extract()
    cleaned_users_data, cleaned_msg_data = cleaning(users_dataframe, msg_dataframe)
    load(cleaned_users_data, cleaned_msg_data)
    print(cleaned_users_data)
    print(cleaned_msg_data)
