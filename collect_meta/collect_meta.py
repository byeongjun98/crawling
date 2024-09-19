# data.go.kr 메타 데이터, 데이터 파일 다운로드 받는 코드 - 박병준
# 다운로드 성공했더니 메타데이터 문제... 23.07.19 11:00
# 거의 다 했다!!! 파일 형태 달라도 일단 다운로드 받아라.. 23.07.19 15:50
### 인턴 때 윈도우 환경에서 만들었던것

import warnings
from logging.config import dictConfig
import logging
import inspect
import datetime
import traceback
from selenium import webdriver
import time
import re
import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.alert import Alert
import os
import pandas as pd
from bs4 import BeautifulSoup
import requests
import shutil

''' log '''
# warnings.filterwarnings(action='ignore')        #경고창 무시하고 숨기기
filePath = os.getcwd()  # 현재 작업 경로 가져오기
fileName = re.split('[.]', inspect.getfile(inspect.currentframe()))[0]  # 문자열을 지정한 패턴으로 분리
filePath, fileName = r'C:\\Python\\crawling', 'collect_meta.py'  # 로그 저장할 파일 이름, 위치 지정
save_file_path = r'C:\\Python\\crawling'
dictConfig({
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)s --- %(message)s',  # 로그에 기록되는 시간, 이름, 메시지
        }
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': '{}/{}_{}.log'.format(filePath, fileName, re.sub('-', '', str(datetime.date.today()))),
            'formatter': 'default',
        },
    },
    'root': {
        'level': 'INFO',
        'handlers': ['file']
    }
})


def log(msg):
    logging.info(msg)


''' main function '''


def main():
    page_num_start = 1  # 시작할 페이지 입력
    page_num_end = 10  # 끝나는 페이지 입력
    driver = start_driver('C:\\Python\\chromedriver_win32\\chromedriver.exe',
                          'https://www.data.go.kr/tcs/dss/selectDataSetList.do?dType=FILE&keyword=&operator=AND&detailKeyword=&publicDataPk=&recmSe=&detailText=&relatedKeyword=&commaNotInData=&commaAndData=&commaOrData=&must_not=&tabId=&dataSetCoreTf=&coreDataNm=&sort=&relRadio=&orgFullName=&orgFilter=&org=&orgSearch=&currentPage={}&perPage=10&brm=&instt=&svcType=%EB%8B%A4%EC%9A%B4%EB%A1%9C%EB%93%9C&kwrdArray=&extsn=CSV&coreDataNmArray=&pblonsipScopeCode='.format(
                              page_num_start))
    # 크롬드라이버 실행, 크롤링 목표 url 입력
    driver.maximize_window()
    dgk_df = pd.DataFrame([], index=range((page_num_end - page_num_start + 1) * 10))
    # 데이터프레임 생성
    col_list = ['title', 'periodicity', 'description', 'url', 'keywords', 'license', 'dateCreated', 'dateModified',
                'datePublished', 'creator_name', 'creator_contactType', 'creator_telephone',
                'distribution_encodingFormat', 'distribution_contentUrl']
    # dict에 저장할 형태
    for col in col_list:
        dgk_df[col] = ''

    for page_num in range(page_num_start, page_num_end + 1):  # 지정한 시작 페이지부터 끝 페이지까지 도는 for문
        log('#### Page Number : {}'.format(page_num))  # 페이지 넘어갈 때마다 그 페이지 번호 받아서 로그 기록

        for i in range(1, 11):  # 페이지마다 데이터셋? 10개이기 때문에 해당 페이지 첫번째 데이터부터 10번째 데이터까지 도는 for문,
            idx = (10 * (page_num - page_num_start)) + i - 1

            try:
                # 데이터 클릭
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
                    (By.XPATH, '//*[@id="fileDataList"]/div[2]/ul/li[{}]/dl/dt/a'.format(i)))).click()
                # 지정한 htmml 태그 경로 중 데이터마다 일련번호가 달라서 그 일련번호는 format 함수로 그때마다 변경

                # 데이터 title, .text() 함수로 타이틀을 문자로 가져온다.

                title_element = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="contents"]/div[2]/div[1]/div[1]/p')))
                title = title_element.text
                dgk_df.loc[idx, 'title'] = title
                time.sleep(3)

                # 데이터 셋마다 다운로드/바로가기 로 버튼의 기능이 다르기 때문에 다운로드 버튼일 경우 클릭한다
                try:

                    click_down = WebDriverWait(driver, 10).until(
                        EC.element_to_be_clickable((By.XPATH, '//*[@id="tab-layer-file"]/div[2]/div[2]/a')))
                    click_down.click()

                except:
                    log('####### no_file : \'{}\' Error'.format(title))

                    # 다운로드 클릭시 불규칙?적으로 alert 창 발생하는데, 이를 처리하기 위한 코드
                try:
                    WebDriverWait(driver, 3).until(EC.alert_is_present())
                    alert = driver.switch_to.alert
                    alert.accept()
                    time.sleep(3)

                    # #다운로드 받은 파일을 원하는 경로로 옮기는 코드 시도중..
                    # downloaded_file_path = 'C:\\Users\\user\\Downloads\\{}.csv'.format(title)
                    # destination_path = 'C:\\Python\\python\\datanuri\\download_data'
                    # shutil.move(downloaded_file_path, destination_path)
                except:
                    log('####### no_alert : \'{}\' Error'.format(title))
            except:
                log('#### Click : \'{}\' Error'.format(title))  # 에러 발생 대비하는 예외처리..?
                log(traceback.format_exc())

            try:
                log('#### #{} Collect : \'{}\''.format(idx, title))
                # 현재 URL
                url = driver.current_url  # 현재 페이지의 URL을 가져와서 url 변수에 저장
                response = requests.get(url)  # 해당 URL에 GET 요청을 보내고, 응답을 받아 response 변수에 저장
                rating_page = response.text  # 응답으로 받은 페이지의 내용을 텍스트 형식으로 rating_page 변수에 저장
                soup = BeautifulSoup(rating_page,
                                     'html.parser')  # BeautifulSoup을 사용하여 rating_page의 HTML 구조를 파싱하여 soup 객체로 생성
                periodicity = soup.find_all('td', 'td custom-cell-border-bottom')[7].text
                # 객체에서 특정 태그와 클래스를 가진 요소들을 찾아 td 태그이면서 td custom-cell-border-bottom 클래스를 가진 요소들을 모두 찾아 리스트 형태로 가져오고,8번째 요소의 텍스트를 가져와 periodicity 변수에 저장

                dgk_df.loc[idx, 'periodicity'] = periodicity
                # dgk_df DataFrame의 idx 행의 'periodicity' 열에 periodicity 값을 할당

                log('######## Periodicity : {}'.format(periodicity))

                url_schema = re.sub('.do', '.json', re.sub('/data/', '/catalog/', url))
                # 해당 데이터의 json형식의 메타데이터는 data.go.kr/data/데이터 일련번호/filedata.do에서 .do를 json으로, data를 catalog로 변경해야 함 이를 구현한 코드

                response = requests.get(url_schema)
                soup_dict = eval(response.text)
                # 가져온 json 형식의 메타데이터를 지정 분류하는 코드
                dgk_df.loc[idx, 'description'] = soup_dict['description']
                dgk_df.loc[idx, 'url'] = soup_dict['url']
                dgk_df.loc[idx, 'keywords'] = soup_dict['keywords'][0]
                dgk_df.loc[idx, 'license'] = soup_dict['license']
                dgk_df.loc[idx, 'dateCreated'] = soup_dict['dateCreated']
                dgk_df.loc[idx, 'dateModified'] = soup_dict['dateModified']
                dgk_df.loc[idx, 'datePublished'] = soup_dict['datePublished']
                dgk_df.loc[idx, 'creator_name'] = soup_dict['creator']['name']
                dgk_df.loc[idx, 'creator_contactType'] = soup_dict['creator']['contactPoint']['contactType']
                dgk_df.loc[idx, 'creator_telephone'] = soup_dict['creator']['contactPoint']['telephone'].replace('+82-',
                                                                                                                 '')
                dgk_df.loc[idx, 'distribution_encodingFormat'] = soup_dict['distribution'][0]['encodingFormat'].lower()
                dgk_df.loc[idx, 'distribution_contentUrl'] = soup_dict['distribution'][0]['contentUrl']

                log('############ Collect Complete!')
                driver.back()
                # 읽어온 데이터 갯수가 100개마다 파일로 갱신
                if idx % 99 == 0:
                    save_path = r'C:\\Python\\crawling\\dgk_data_{}_{}.csv'.format(page_num_start, page_num)
                    dgk_df.to_csv(save_path, index=False, encoding='UTF-8-SIG')
                    log('#### To CSV \'{}\''.format(save_path))
            except:
                log('######## Collect : \'{}\' Error'.format(title))
                log(traceback.format_exc())
                driver.back()

        click_next = {0: 12, 1: 3, 2: 4, 3: 5, 4: 6, 5: 7, 6: 8, 7: 9, 8: 10, 9: 11}
        WebDriverWait(driver, 100).until(EC.element_to_be_clickable(
            (By.XPATH, '/html/body/div[2]/div/div/div/div[10]/nav/a[{}]'.format(click_next[page_num % 10])))).click()
        log('#### Next Page')


''' functions '''


# 크롬 드라이버 실행 함수
def start_driver(driver_path, url, down_path=None):
    try:
        log('#### Start Driver')
        chrome_options = webdriver.ChromeOptions()
        # 서버 전용 옵션 활성화
        # chrome_options.add_argument('--headless')
        # chrome_options.add_argument('--no-sandbox')
        # chrome_options.add_argument('--disable-dev-shm-usage')

        # 다운로드 경로 변경 및 기타 옵션 설정
        if down_path is None:
            prefs = {
                'download.prompt_for_download': False,
                'download.directory_upgrade': True
            }
            chrome_options.add_experimental_option('prefs', prefs)
        else:
            prefs = {
                'download.default_directory': down_path,
                'download.prompt_for_download': False,
                'download.directory_upgrade': True,
                'safebrowsing.enabled': True
            }
            chrome_options.add_experimental_option('prefs', prefs)

        # 드라이버 시작
        driver = webdriver.Chrome(service=Service(driver_path), options=chrome_options)
        driver.implicitly_wait(10)  # 대기 시작 설정
        driver.get(url)  # URL 적용

        # 로딩 대기
        return driver
    except:
        log('######## Start Driver Error')
        log(traceback.format_exc())


''' main '''
if __name__ == '__main__':
    # 시간 계산
    start_time = time.time()

    main()

    log('#### ===================== Time =====================')
    log('#### {:.3f} seconds'.format(time.time() - start_time))
    log('#### ================================================')