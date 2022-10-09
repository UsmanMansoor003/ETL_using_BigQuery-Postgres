
# Author - Muhammad Usman - 09/09/2022


from datetime import datetime, date
import pandas as pd
import requests
from sqlalchemy import create_engine


# main request function
def main_request(baseurl, endpoint, headers):
    # using requests to call API data
    r = requests.get(baseurl + endpoint, headers=headers)
    # returning data in json format
    return r.json()


def anonymization_birthdate(birthdate):
    return '{:%Y-01-01}'.format(datetime.strptime(birthdate, '%Y-%m-%dT%H:%M:%S.%fZ'))


def calculate_age(birthdate):
    born = datetime.strptime(birthdate, '%Y-%m-%dT%H:%M:%S.%fZ')
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))


def extract_domain(email):
    # slicing domain name using slicing
    res = email[email.index('@') + 1:]
    # printing result
    return str(res)


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
    normalize_users_dataframe = pd.json_normalize(users_data)

    # Subscription Data
    normalize_subscription_dataframe = pd.json_normalize(users_data, record_path=['subscription'], meta=['id'])
    normalize_subscription_dataframe.rename(columns={'id': 'userid'}, inplace=True)

    # MESSAGE DATA
    normalize_msg_dataframe = pd.json_normalize(msg_data)
    normalize_msg_dataframe.drop(['message'], axis=1, inplace=True)

    return normalize_users_dataframe, normalize_msg_dataframe, normalize_subscription_dataframe


def cleaning(users_data):
    # Calculating Agr form the BirthData
    users_data['age'] = users_data['birthDate'].apply(calculate_age)
    users_data['birthDate'] = users_data['birthDate'].apply(anonymization_birthdate)

    # Remove Subscription and all PII Id's and un-necessary columns
    users_data.drop(['subscription', 'firstName', 'lastName', 'address', 'zipCode', 'profile.profession'],
                    axis=1, inplace=True)
    # Rename Profile Columns
    users_data.rename(
        columns={'profile.gender': 'gender', 'profile.isSmoking': 'isSmoking', 'profile.income': 'income'},
        inplace=True)
    # Extract Domain name from Emails
    users_data['email'] = users_data['email'].apply(extract_domain)

    return users_data


def load(cleaned_users_data_to_load, subscription_data_to_load, msg_data_to_load):
    engine = create_engine('postgresql://wrloehng:kCZuq0Qmn9SnE8iEunTlyJnl47XqdR2w@abul.db.elephantsql.com/wrloehng')
    conn = engine.connect()

    # DROP TABLE IF ALREADY EXISTS
    engine.execute('DROP TABLE "public"."message";')
    engine.execute('DROP TABLE "public"."subscription";')
    engine.execute('DROP TABLE "public"."user";')

    # Load Users table into postgres
    cleaned_users_data_to_load.to_sql('user', engine, if_exists='replace', index=False)
    engine.execute('ALTER TABLE "public"."user" ADD PRIMARY KEY("id");')

    # Load subscription table into postgres
    subscription_data_to_load.to_sql('subscription', engine, if_exists='replace', index=False)
    engine.execute('ALTER TABLE "public"."subscription" ADD FOREIGN KEY("userid") REFERENCES "public"."user"("id");')

    # Load messages table into postgres
    msg_data_to_load.to_sql('message', engine, if_exists='replace', index=False)
    engine.execute('ALTER TABLE "public"."message" ADD PRIMARY KEY("id");')
    engine.execute('ALTER TABLE "public"."message" ADD FOREIGN KEY("receiverId") REFERENCES "public"."user"("id");')
    engine.execute('ALTER TABLE "public"."message" ADD FOREIGN KEY("senderId") REFERENCES "public"."user"("id");')

    conn.close()
    engine.dispose()


if __name__ == '__main__':
    users_dataframe, msg_dataframe, subscription_dataframe = extract()
    cleaned_users_data = cleaning(users_dataframe)
    load(cleaned_users_data, subscription_dataframe, msg_dataframe)
    print(cleaned_users_data)
    print(subscription_dataframe)
    print(msg_dataframe)


