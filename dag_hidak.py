import requests
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from pykospacing import Spacing


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 4),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'hidak_scraping_dag',
    default_args=default_args,
    description='A simple DAG to scrape naver data',
    schedule_interval=timedelta(days=1),
)



def process_all_departments():
    # 과 이름에 대한 코드 매핑
    hidak_all = {
        '가정의학과': 'PF000',
        '소아청소년과': 'PD000',
        '이비인후과': 'PE000',
        '소화기내과': 'PMG00',
        '호흡기내과': 'PMP00',
        '감염내과': 'PMI00',
        '알레르기내과': 'PMA00',
        '내분비내과': 'PME00',
        '순환기내과': 'PMC00',
        '신장내과': 'PMN00',
        '류마티스내과': 'PMR00',
        '혈액종양내과': 'PMO00',
        '성형외과': 'PA000',
        '정형외과': 'PO000',
        '신경외과': 'PB000',
        '대장항문': 'PGI00',
        '흉부외과': 'PC000',
        '외과': 'PG000',
        '피부과': 'PS000',
        '안과': 'PH000',
        '치과': 'PV000',
        '비뇨의학과': 'PU000',
        '산부인과': 'PY000',
        '한방과': 'PL000',
        '신경과': 'PN000',
        '재활의학과': 'PR000',
        '정신건강의학과': 'PP000',
        '응급의학과': 'PJ000',
        '마취통증의학과': 'PT000',
        '방사선종양학과': 'PX000',
        '영상의학과': 'PK000',
        '진담검사의학과': 'PQL00',
        '작업환경의학과': 'PQ000',
    }

    head = {'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}

    # 어제 날짜 구하기
    yesterday = datetime.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime('%Y-%m-%d')

    for department, department_code in hidak_all.items():
        link_list = []

        for page in range(1, 2): 
            url = f"https://mobile.hidoc.co.kr/healthqna/part/list?code={department_code}&page={page}"
            r = requests.get(url, headers=head)
            bs = BeautifulSoup(r.text, 'html.parser')

            data = bs.find("section", class_='contents').find_all("a") 

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
            today = datetime.today().strftime('%Y-%m-%d')
            new_file_path = f"/opt/airflow/dags/hidak_link_/{department_code}_{department}_{today}.json"

            with open(new_file_path, 'w', encoding='utf-8') as f:
                json.dump(link_list, f, ensure_ascii=False, indent=4)

            print(f"{department}의 새로운 데이터가 저장되었습니다: {new_file_path}")
        else:
            print(f"{department}의 새로운 데이터가 없습니다.")




def process_all_qna():
    today = datetime.today().strftime('%Y-%m-%d')
    for json_file in os.listdir("/opt/airflow/dags/hidak_link_"):
        if json_file.endswith(".json") and today in json_file:
            with open(os.path.join("/opt/airflow/dags/hidak_link_", json_file), 'r') as f:
                links_list = json.load(f)
            department = json_file.split("_")[1].split(".")[0]  # 파일명에서 부서명 추출, 확장자 제거
            list1 = links_list

            head = { 'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"}
            data = []
            for link in list1:
                url = f"https://mobile.hidoc.co.kr/healthqna/part/{link}"
                r = requests.get(url, headers=head)
                bs = BeautifulSoup(r.text, 'html.parser')
                date_data = bs.find("span", class_='date')
                date = date_data.text
                title_data = bs.select('.tit_qna')
                pattern = r'<strong class="tit_qna">\s*(.*?)\s*<div class="qna_info">'
                title_data_ = str(title_data[0])
                title = re.search(pattern, title_data_)
                if title:
                    title_ = title.group(1)
                else:
                    title_ = "매칭되는 부분이 없습니다."
                question_data = bs.find('div', class_='desc')   
                question = question_data.text.strip() 
                doctor_info = bs.find_all("strong", class_='link_doctor')
                doctor_list = []
                for x in range(len(doctor_info)):
                    doctor_list.append(doctor_info[0].text)
                doc_hospital = bs.find_all("span", class_ ='txt_clinic')
                hospital_list = []
                for x in range(len(doc_hospital)):
                    hospital_list.append(doc_hospital[0].text)
                a = bs.findAll("div", class_ = 'desc')
                answer = []
                for x in range(1,len(a)):
                    answer.append(a[x].text.strip())
                data.append({
                    'Date': date,
                    'Title': title_,
                    'Question': question,
                    'Doctors': doctor_list,
                    'Hospitals': hospital_list,
                    'Answers': answer
                })
            output_directory = "/opt/airflow/dags/hidak_qna_"
            output_file = os.path.join(output_directory, f"{department}_{today}_QNA.json")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"{department}의 새로운 데이터가 저장되었습니다:")



spacing = Spacing()
# json_directory = "/Users/sseungpp/dev/hidak_dag/qnatest"
# output_directory = "/Users/sseungpp/dev/hidak_dag/qna_protest"

def remove_greeting2(text):
    return re.sub(r'하이닥.*?입니다\.', '', text)
def remove_greeting1(text):
    return re.sub(r'^안녕하세요\.', '', text)
def remove_special_chars(text):
    return text.replace('\\xa0', '')

def preprocess_json(data_list):
    preprocessed_data = []
    for data in data_list:
        if not data['Date']:  # Date가 비어있는 경우
            preprocessed_data = data
            return preprocessed_data
        else:
            # 전처리 적용
            data['Title'] = spacing(data['Title'])
            data['Question'] = spacing(data['Question'])
            data['Answers'] = ''.join(data['Answers'])  # 리스트를 문자열로 변환
            data['Question'] = remove_greeting1(data['Question'])  # 인사말 제거
            data['Answers'] = remove_greeting1(data['Answers'])  # 인사말 제거
            data['Answers'] = remove_greeting2(data['Answers'])  # 인사말 제거
            data['Answers'] = remove_special_chars(data['Answers'])  # 특수 문자 제거
            data['Answers'] = spacing(data['Answers'])
            data['Question'] = remove_special_chars(data['Question'])  # 특수 문자 제거
            if '삭제' not in data['Question']:  # '삭제' 키워드가 없는 경우만 추가
                preprocessed_data.append(data)
    return preprocessed_data




def preprocess_json_files(json_dir, output_dir):
    for json_file in os.listdir(json_dir):
        if json_file.endswith(".json") and yesterday_str in json_file:
            json_path = os.path.join(json_dir, json_file)
            with open(json_path, 'r', encoding='utf-8') as f:
                data_list = json.load(f)
            preprocessed_data = preprocess_json(data_list)
        
            output_filename = os.path.splitext(json_file)[0] + "_pros.json"
            output_path = os.path.join(output_dir, output_filename)
            with open(output_path, 'w', encoding='utf-8') as outfile:
                json.dump(preprocessed_data, outfile, default=str, ensure_ascii=False, indent=4)
            print(f"전처리된 데이터가 {output_path}에 저장되었습니다.")

yesterday = datetime.today() - timedelta(days=1)
yesterday_str = yesterday.strftime('%Y-%m-%d')





json_directory = "/opt/airflow/dags/hidak_qna_"
output_directory = "/opt/airflow/dags/hidak_processing_"


today_link1 = PythonOperator(
    task_id = "today_link1",
    python_callable = process_all_departments,
    dag=dag,
)


today_qna1 = PythonOperator(
    task_id = "today_qna1",
    python_callable = process_all_qna,
    dag=dag,
)


preprocess_task = PythonOperator(
    task_id='preprocess_json_files',
    python_callable=preprocess_json_files,
    op_args=[json_directory, output_directory],
    dag=dag,
)

today_link1 >> today_qna1 >> preprocess_task  


