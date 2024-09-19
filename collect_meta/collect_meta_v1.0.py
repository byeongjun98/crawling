

# import warnings
from logging.config import dictConfig
import logging
# import inspect
import datetime
import traceback
from selenium import webdriver
import time
import re
# import numpy as np
import pandas as pd
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.common.alert import Alert
# import os
from bs4 import BeautifulSoup
import requests
# import shutil
from webdriver_manager.chrome import ChromeDriverManager

''' log '''
# warnings.filterwarnings(action='ignore')        # 경고창 무시하고 숨기기
filePath = '/Users/parkbyeongjun/datanuri/crawling'  # 로그 및 데이터 저장 경로
fileName = 'collect_meta'  # 로그 파일 이름

dictConfig({
    'version': 1,
    'formatters': {
        'default': {
            'format': '[%(asctime)s] %(levelname)s --- %(message)s',  # 로그 형식
        }
    },
    'handlers': {
        'file': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': f'{filePath}/{fileName}_{datetime.date.today().strftime("%Y%m%d")}.log',
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


''' functions '''


# 크롬 드라이버 실행 함수
def start_driver(url, down_path=None):
    try:
        log('#### Start Driver')
        chrome_options = webdriver.ChromeOptions()

        # 서버 전용 옵션 활성화 (필요 시 주석 해제)
        # chrome_options.add_argument('--headless')
        # chrome_options.add_argument('--no-sandbox')
        # chrome_options.add_argument('--disable-dev-shm-usage')

        # 다운로드 경로 설정 및 기타 옵션
        if down_path is None:
            prefs = {
                'download.prompt_for_download': False,
                'download.directory_upgrade': True
            }
        else:
            prefs = {
                'download.default_directory': down_path,
                'download.prompt_for_download': False,
                'download.directory_upgrade': True,
                'safebrowsing.enabled': True
            }
        chrome_options.add_experimental_option('prefs', prefs)

        # webdriver_manager를 사용하여 ChromeDriver 설치 및 경로 설정
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.implicitly_wait(10)  # 암묵적 대기 시간 설정
        driver.get(url)  # URL 접속

        log('#### Driver started successfully')
        return driver
    except Exception as e:
        log('######## Start Driver Error')
        log(traceback.format_exc())
        return None  # 명시적으로 None 반환


def collect_data(driver, dgk_df, page_num_start, page_num):
    for i in range(1, 11):  # 페이지마다 10개의 데이터셋
        idx = (10 * (page_num - page_num_start)) + i - 1
        title = ''

        try:
            # 데이터 클릭
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
                (By.XPATH, f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/dl/dt/a'))).click()

            # 데이터 title 가져오기
            title_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="contents"]/div[2]/div[1]/div[1]/p')))
            title = title_element.text
            dgk_df.loc[idx, 'title'] = title
            time.sleep(1)  # 불필요한 대기 시간을 줄임

            # 다운로드 버튼 클릭 시도
            try:
                click_down = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="tab-layer-file"]/div[2]/div[2]/a')))
                click_down.click()
            except:
                log(f'####### no_file : \'{title}\' Error')

            # Alert 처리
            try:
                WebDriverWait(driver, 3).until(EC.alert_is_present())
                alert = driver.switch_to.alert
                alert.accept()
                time.sleep(1)
            except:
                log(f'####### no_alert : \'{title}\'')

        except Exception as e:
            log(f'#### Click : \'{title}\' Error')
            log(traceback.format_exc())
            continue  # 다음 데이터로 넘어감

        try:
            log(f'#### # {idx} Collect : \'{title}\'')
            # 현재 URL
            url = driver.current_url
            response = requests.get(url)
            rating_page = response.text
            soup = BeautifulSoup(rating_page, 'html.parser')

            # 메타데이터 수집
            periodicity = soup.find_all('td', 'td custom-cell-border-bottom')[7].text
            dgk_df.loc[idx, 'periodicity'] = periodicity
            log(f'######## Periodicity : {periodicity}')

            url_schema = re.sub('.do', '.json', re.sub('/data/', '/catalog/', url))
            response = requests.get(url_schema)
            soup_dict = response.json()  # eval 대신 json 사용

            # 메타데이터 할당
            dgk_df.loc[idx, 'description'] = soup_dict.get('description', '')
            dgk_df.loc[idx, 'url'] = soup_dict.get('url', '')
            dgk_df.loc[idx, 'keywords'] = soup_dict.get('keywords', [''])[0]
            dgk_df.loc[idx, 'license'] = soup_dict.get('license', '')
            dgk_df.loc[idx, 'dateCreated'] = soup_dict.get('dateCreated', '')
            dgk_df.loc[idx, 'dateModified'] = soup_dict.get('dateModified', '')
            dgk_df.loc[idx, 'datePublished'] = soup_dict.get('datePublished', '')
            dgk_df.loc[idx, 'creator_name'] = soup_dict.get('creator', {}).get('name', '')
            dgk_df.loc[idx, 'creator_contactType'] = soup_dict.get('creator', {}).get('contactPoint', {}).get(
                'contactType', '')
            dgk_df.loc[idx, 'creator_telephone'] = soup_dict.get('creator', {}).get('contactPoint', {}).get('telephone',
                                                                                                            '').replace(
                '+82-', '')
            dgk_df.loc[idx, 'distribution_encodingFormat'] = soup_dict.get('distribution', [{}])[0].get(
                'encodingFormat', '').lower()
            dgk_df.loc[idx, 'distribution_contentUrl'] = soup_dict.get('distribution', [{}])[0].get('contentUrl', '')

            log('############ Collect Complete!')
            driver.back()

            # 데이터 저장 (100개마다)
            if idx % 99 == 0:
                save_path = f'{filePath}/dgk_data_{page_num_start}_{page_num}.csv'
                dgk_df.to_csv(save_path, index=False, encoding='UTF-8-SIG')
                log(f'#### To CSV \'{save_path}\'')

        except Exception as e:
            log(f'######## Collect : \'{title}\' Error')
            log(traceback.format_exc())
            driver.back()


def main():
    page_num_start = 1  # 시작 페이지
    page_num_end = 10  # 끝 페이지
    url_template = 'https://www.data.go.kr/tcs/dss/selectDataSetList.do?dType=FILE&keyword=&operator=AND&detailKeyword=&publicDataPk=&recmSe=&detailText=&relatedKeyword=&commaNotInData=&commaAndData=&commaOrData=&must_not=&tabId=&dataSetCoreTf=&coreDataNm=&sort=&relRadio=&orgFullName=&orgFilter=&org=&orgSearch=&currentPage={}&perPage=10&brm=&instt=&svcType=%EB%8B%A4%EC%9A%B4%EB%A1%9C%EB%93%9C&kwrdArray=&extsn=CSV&coreDataNmArray=&pblonsipScopeCode='

    driver = start_driver(url_template.format(page_num_start))

    if driver is None:
        log('#### Driver failed to start. Exiting program.')
        return  # 드라이버가 시작되지 않으면 프로그램 종료

    driver.maximize_window()
    dgk_df = pd.DataFrame(columns=[
        'title', 'periodicity', 'description', 'url', 'keywords', 'license',
        'dateCreated', 'dateModified', 'datePublished', 'creator_name',
        'creator_contactType', 'creator_telephone', 'distribution_encodingFormat',
        'distribution_contentUrl'
    ])
    # 데이터프레임 인덱스 자동 생성

    for page_num in range(page_num_start, page_num_end + 1):
        log(f'#### Page Number : {page_num}')
        collect_data(driver, dgk_df, page_num_start, page_num)

        # 다음 페이지로 이동
        try:
            click_next = {1: 3, 2: 4, 3: 5, 4: 6, 5: 7, 6: 8, 7: 9, 8: 10, 9: 11, 10: 12}
            next_page_button = WebDriverWait(driver, 10).until(EC.element_to_be_clickable(
                (By.XPATH, f'/html/body/div[2]/div/div/div/div[10]/nav/a[{click_next.get(page_num % 10, 3)}]')))
            next_page_button.click()
            log('#### Next Page')
        except Exception as e:
            log(f'#### Failed to navigate to next page: {page_num}')
            log(traceback.format_exc())
            break  # 다음 페이지로 이동 실패 시 루프 종료

    # 최종 데이터 저장
    save_path = f'{filePath}/dgk_data_{page_num_start}_{page_num_end}.csv'
    dgk_df.to_csv(save_path, index=False, encoding='UTF-8-SIG')
    log(f'#### Final CSV saved at \'{save_path}\'')

    driver.quit()
    log('#### Driver closed successfully')


''' main '''
if __name__ == '__main__':
    # 시간 계산
    start_time = time.time()

    main()

    log('#### ===================== Time =====================')
    log(f'#### {time.time() - start_time:.3f} seconds')
    log('#### ================================================')
