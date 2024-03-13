import requests
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from dotenv import load_dotenv
import csv

# from pykospacing import Spacing
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

from pytz import timezone

local_timezone = timezone("Asia/Seoul")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": local_timezone.localize(datetime(2024, 3, 10, 23, 45)),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "hidak_scraping_dag",
    default_args=default_args,
    description="A simple DAG to scrape naver data",
    schedule_interval=timedelta(days=1),
)


def process_all_departments():

    folder_path = "./hidak/hidak_link_"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"폴더 '{folder_path}'가 생성되었습니다.")
    else:
        print(f"폴더 '{folder_path}'가 이미 존재합니다.")
    folder_path = "./hidak/hidak_qna_"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"폴더 '{folder_path}'가 생성되었습니다.")
    else:
        print(f"폴더 '{folder_path}'가 이미 존재합니다.")
    folder_path = "./hidak/hidak_processing_"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"폴더 '{folder_path}'가 생성되었습니다.")
    else:
        print(f"폴더 '{folder_path}'가 이미 존재합니다.")

    # 과 이름에 대한 코드 매핑

    head = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    }

    # 어제 날짜 구하기
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    link_list = []

    for page in range(1, 60):
        # url = f"https://mobile.hidoc.co.kr/healthqna/part/list?code={department_code}&page={page}"
        url = f"https://mobile.hidoc.co.kr/healthqna/list?page={page}"
        r = requests.get(url, headers=head)
        bs = BeautifulSoup(r.text, "html.parser")

        data = bs.find("section", class_="contents").find_all("a")

        pattern = r'href="(view[^"]+)"'
        for tag in data:
            tag_str = str(tag)
            match = re.search(pattern, tag_str)
            if match:
                link = match.group(1)
                link_list.append(link)

    link_list = list(set(link_list))
    if link_list:
        # 새로운 파일명 생성 (오늘 날짜)
        new_file_path = f"./hidak/hidak_link_/{yesterday_str}_link.json"

        with open(new_file_path, "w", encoding="utf-8") as f:
            json.dump(link_list, f, ensure_ascii=False, indent=4)


def process_all_qna_v2():
    today = datetime.today().strftime("%Y-%m-%d")
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    for json_file in os.listdir("./hidak/hidak_link_"):
        if json_file.endswith(".json") and yesterday_str in json_file:
            with open(os.path.join("./hidak/hidak_link_", json_file), "r") as f:
                links_list = json.load(f)
            list1 = links_list

            head = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            }
            data = []
            for link in list1:
                url = f"https://mobile.hidoc.co.kr/healthqna/part/{link}"
                r = requests.get(url, headers=head)
                bs = BeautifulSoup(r.text, "html.parser")
                date_data = bs.find("span", class_="date")
                date = date_data.text
                date = date.replace(".", "-")
                title_data = bs.select(".tit_qna")
                pattern = r'<strong class="tit_qna">\s*(.*?)\s*<div class="qna_info">'
                title_data_ = str(title_data[0])
                title = re.search(pattern, title_data_)
                if title:
                    title_ = title.group(1)
                else:
                    title_ = "매칭되는 부분이 없습니다."
                question_data = bs.find("div", class_="desc")
                question = question_data.text.strip()
                doctor_info = bs.find_all("strong", class_="link_doctor")
                doctor_list = []
                for x in range(len(doctor_info)):
                    doctor_list.append(doctor_info[x].text)
                doc_hospital = bs.find_all("span", class_="txt_clinic")
                hospital_list = []
                for x in range(len(doc_hospital)):
                    hospital_list.append(doc_hospital[x].text)
                a = bs.findAll("div", class_="desc")
                answer = []
                for x in range(1, len(a)):
                    answer.append(a[x].text.strip())

                if date == yesterday_str:
                    data.append(
                        {
                            "date": date,
                            "title": title_,
                            "question": question,
                            "doctors": doctor_list,
                            "hospitals": hospital_list,
                            "answer": answer,
                        }
                    )
                # print(data)
            if len(data) == 0:
                data = [
                    (
                        {
                            "date": "",
                            "title": "",
                            "question": "",
                            "doctors": [],
                            "hospitals": [],
                            "answer": [],
                        }
                    )
                ]
            output_file = f"./hidak/hidak_qna_/{yesterday_str}_qna.csv"
            with open(output_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    ["date", "title", "question", "doctors", "hospitals", "answer"]
                )
                for item in data:
                    writer.writerow(
                        [
                            item["date"],
                            item["title"],
                            item["question"],
                            ", ".join(item["doctors"]),
                            ", ".join(item["hospitals"]),
                            ", ".join(item["answer"]),
                        ]
                    )


# json_directory = "/Users/sseungpp/dev/hidak_dag/qnatest"
# output_directory = "/Users/sseungpp/dev/hidak_dag/qna_protest"


def remove_greeting2(text):
    return re.sub(r"하이닥.*?입니다\.", "", text)


def remove_greeting1(text):
    return re.sub(r"안녕하세요\.", "", text)


def remove_special_chars(text):
    return text.replace("\\xa0", "")


# spacing = Spacing()
def preprocess_json(df):
    preprocessed_data = []

    for index, row in df.iterrows():
        data = {}
        data["date"] = row["date"]
        data["title"] = row["title"]
        data["question"] = remove_greeting1(row["question"])  # 인사말 제거
        data["doctors"] = row["doctors"]
        data["hospitals"] = row["hospitals"]
        data["answer"] = "".join(row["answer"])  # 리스트를 문자열로 변환
        data["answer"] = remove_greeting1(data["answer"])  # 인사말 제거
        data["answer"] = remove_greeting2(data["answer"])  # 인사말 제거
        data["answer"] = remove_special_chars(data["answer"])  # 특수 문자 제거
        data["question"] = remove_special_chars(data["question"])  # 특수 문자 제거

        if "삭제" not in data["question"]:  # '삭제' 키워드가 없는 경우만 추가
            preprocessed_data.append(data)

    return preprocessed_data


def preprocess_json_files():
    csv_dir = "./hidak/hidak_qna_/"
    output_dir = "./hidak/hidak_processing_/"
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    print(yesterday_str)
    for csv_file in os.listdir(csv_dir):
        if csv_file.endswith(".csv") and yesterday_str in csv_file:
            csv_path = os.path.join(csv_dir, csv_file)
            df = pd.read_csv(csv_path)  # CSV 파일 읽기
            preprocessed_data = preprocess_json(df)  # 데이터프레임 전처리 함수 호출

            output_filename = f"{yesterday_str}_qna_pros.csv"
            output_path = os.path.join(output_dir, output_filename)
            print(output_path)
            df = pd.DataFrame(preprocessed_data)
            df.to_csv(output_path, index=False, encoding="utf-8")
            print(f"전처리된 데이터가 {output_path}에 저장되었습니다.")


def insert_data_to_postgres(**kwargs):
    csv_file_path = "/opt/airflow/hidak/hidak_processing_/"
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    # # PostgreSQL 연결 설정
    # user = 'your_username'
    # password = 'your_password'
    # host = 'localhost'  # 또는 데이터베이스 호스트 주소
    # port = '5432'  # 또는 데이터베이스 포트 번호
    # dbname = 'your_database_name'

    # engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')

    # PostgreSQL 연결 설정
    # 로컬에서 실행할 경우 아래 ip로 설정
    # engine = create_engine('postgresql+psycopg2://encore:hadoop@54.180.156.162:5432/qna')

    # ec2 인스턴스에서 실행할땐 아래 ip로 설정
    engine = create_engine("postgresql+psycopg2://encore:hadoop@172.31.13.180:5432/qna")

    for file in os.listdir(csv_file_path):
        if file.endswith(".csv") and yesterday_str in file:
            # json 파일의 전체 경로
            file_path = os.path.join(csv_file_path, file)

            # json 파일을 DataFrame으로 읽기
            df = pd.read_csv(file_path)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            df["title"] = df["title"].astype("str")
            df["question"] = df["question"].astype("str")
            df["doctors"] = df["doctors"].astype("str")
            df["hospitals"] = df["hospitals"].astype("str")
            df["answer"] = df["answer"].astype("str")
            # DataFrame을 PostgreSQL 테이블에 삽입
            df.to_sql("processed_data_hidak", engine, if_exists="append", index=False)


# .env 파일에서 환경변수 로드
load_dotenv()

# 환경변수에서 Slack Webhook URL 가져오기
slack_webhook_url = os.getenv("SLACK_WEBHOOK_URL_MEDICAL")


def send_slack_notification(ds, **kwargs):
    message = "Airflow DAG 실행 완료: {}".format(ds)
    data = {"text": message}

    response = requests.post(slack_webhook_url, json=data)

    if response.status_code != 200:
        raise ValueError(
            "Slack에 메시지를 보내는 데 실패했습니다. 응답: {}".format(response.text)
        )


today_link1 = PythonOperator(
    task_id="today_link1",
    python_callable=process_all_departments,
    dag=dag,
)


today_qna1 = PythonOperator(
    task_id="today_qna1",
    python_callable=process_all_qna_v2,
    dag=dag,
)


preprocess_task = PythonOperator(
    task_id="preprocess_json_files",
    python_callable=preprocess_json_files,
    dag=dag,
)

insert_to_DB = PythonOperator(
    task_id="insert_to_DB",
    python_callable=insert_data_to_postgres,
    dag=dag,
)

send_notification = PythonOperator(
    task_id="send_slack_notification", python_callable=send_slack_notification, dag=dag
)


today_link1 >> today_qna1 >> preprocess_task >> insert_to_DB >> send_notification
