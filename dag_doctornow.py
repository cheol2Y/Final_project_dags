from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
from dotenv import load_dotenv
from pytz import timezone
import re
import pandas as pd
import requests
import json
import psycopg2
from sqlalchemy import create_engine
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from selenium import webdriver
from selenium.webdriver.common.by import By
import time
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains        


local_timezone = timezone(
    "Asia/Seoul"
) 

# 환경 변수 로드
load_dotenv()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": local_timezone.localize(datetime(2024, 3, 10, 23, 35)),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "doctornow_scraping_dag",
    default_args=default_args,
    description="A DAG for scraping doctornow questions",
    schedule_interval=timedelta(days=1),
)






def clean_text(text):
    # HTML 태그 제거
    text = re.sub(r'<[^>]+>', '', text)
    # 이메일 주소 제거
    text = re.sub(r'\S+@\S+', '', text)
    # URL 제거
    text = re.sub(r'http\S+', '', text)
    # 공백 제거
    text = re.sub(r'\s+', ' ', text)
    # 특수 문자 제거
    text = re.sub(r'[^\w\s]', '', text)
    return text

# 문장 분리 및 정규화
def split_sentences(text):
    # 문장 분리
    sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s', text)
    # 각 문장 정규화
    sentences = [clean_text(sentence) for sentence in sentences]
    return ' '.join(sentences)

def convert_date_format(date_str):
    return date_str.replace(".", "-")

def scrape_questions():
    options = Options()
    # 옵션 설정을 함수로 분리할 수 있지만, 여기서는 직접 설정합니다.
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_prefs = {"profile.default_content_settings": {"images": 2}, "profile.managed_default_content_settings": {"images": 2}}
    options.experimental_options["prefs"] = chrome_prefs

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.implicitly_wait(10)  # 대기 시간 설정
    qid_list = []
    title_list= []
    question_list = []
    answer_list = []

    try:

        
        for i in range(1, 601):  # 최대 200개의 항목을 확인
            try:
                driver.get('https://doctornow.co.kr/content/qna')
                driver.execute_script("document.body.style.zoom='50%'")
                time.sleep(1)
                selector = f'#__next > div.sc-fc54fed-0.felfNr > div > div.sc-fc54fed-2.eTPIWh > div > section > section > section > div:nth-child(1) > article:nth-child({i}) > div'
                for _ in range(int(i / 20 )):
                    # 페이지 맨 아래로 스크롤
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    # 스크롤 사이에 충분한 로딩 시간 대기
                    time.sleep(0.5)
                element = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                driver.execute_script("arguments[0].click();", element)
                print('클릭성공')
                time.sleep(0.5)  # 동적 컨텐츠 로딩 대기

                canonical_link = driver.find_element(By.CSS_SELECTOR, 'link[rel="canonical"]').get_attribute('href')
                qna_id = re.search(r'qna/(.+)', canonical_link).group(1) if re.search(r'qna/(.+)', canonical_link) else None

                if qna_id:
                    print(f"QnA ID: {qna_id}")
                    qid_list.append(qna_id)
                
                selectors_to_lists = {
                    """//*[@id="__next"]/div[2]/div/div[2]/div/section/section/div[2]/section/article/div[1]/h1""": title_list,
                    """//*[@id="__next"]/div[2]/div/div[2]/div/section/section/div[2]/section/article/div[1]/p""": question_list,
                    """//*[@id="__next"]/div[2]/div/div[2]/div/section/section/div[2]/div[2]/ul/li/div[2]/p""": answer_list,
                }
                for selector, data_list in selectors_to_lists.items():
                    elements = driver.find_elements(By.XPATH, selector)
                    for element in elements[:1]:

                        data_list.append(element.text)
                div_element = driver.find_element(By.XPATH, """//*[@id="__next"]/div[2]/div/div[2]/div/section/section/div[2]/section/article/div[1]/div[1]/div""")
                # div 요소의 텍스트 가져오기
                div_text = div_element.text
                print(div_text)
                    # "1일 전" 문자열이 포함되어 있는지 확인
                if "1일 전" in div_text:    
                    break
                print(i)
            except Exception as e:
                print(f"Error processing item {i}: {str(e)}")
                continue  # 실패 시 다음 반복으로

    finally:
        driver.quit()
        
    df = pd.DataFrame({
    "title": title_list,
    "question": question_list,
    "answer": answer_list,
    })
    current_date = datetime.now().strftime("%Y-%m-%d")
    df['date'] = current_date

    folder_name="./csv_folder/doctornow_realtime"
    csv_file_path = os.path.join(folder_name, f'doctornow_realtime_{current_date}.csv')
    
    # df = pd.read_csv(csv_file_path)
    df['title'] = df['title'].apply(clean_text)
    df['question'] = df['question'].apply(clean_text)
    df['answer'] = df['answer'].apply(split_sentences)
    df['date'] = df['date'].apply(convert_date_format)
    
    csv_out_path = os.path.join(folder_name, f'doctornow_realtime_{current_date}_pros.csv')
    df.to_csv(csv_out_path, index=False) 
    
    
    # PostgreSQL 연결 설정
    # 로컬에서 실행할 경우 아래 ip로 설정
    # engine = create_engine('postgresql+psycopg2://encore:hadoop@54.180.156.162:5432/qna')

    # ec2 인스턴스에서 실행할땐 아래 ip로 설정
    engine = create_engine("postgresql+psycopg2://encore:hadoop@172.31.13.180:5432/qna")



    # DataFrame을 PostgreSQL 테이블에 삽입
    df.to_sql("processed_data_doctornow", engine, if_exists="append", index=False)


# .env 파일에서 환경변수 로드
load_dotenv()

# 환경변수에서 Slack Webhook URL 가져오기
slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL_MEDICAL')

def send_slack_notification(ds, **kwargs):
    message = "Airflow DAG 실행 완료: {}".format(ds)
    data = {'text': message}
    
    response = requests.post(slack_webhook_url, json=data)
    
    if response.status_code != 200:
        raise ValueError(
            'Slack에 메시지를 보내는 데 실패했습니다. 응답: {}'.format(response.text)
        )




scrape_task = PythonOperator(
    task_id="scrape_questions",
    python_callable=scrape_questions,
    dag=dag,
)

# send_notification = PythonOperator(
#     task_id='send_slack_notification',
#     python_callable=send_slack_notification,
#     dag=dag)

scrape_task #>> send_notification