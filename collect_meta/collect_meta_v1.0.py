import time
import traceback
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import requests
from bs4 import BeautifulSoup
import re


# 로그 경로 및 파일 이름 설정
filePath = '/Users/parkbyeongjun/datanuri/crawling'  # 로그 및 데이터 저장 경로
fileName = 'collect_meta'  # 로그 파일 이름


def log(message):
    """ 로그 함수: 로그 파일에 메시지를 기록 """
    with open(f'{filePath}/{fileName}.log', 'a') as log_file:
        log_file.write(f'{time.strftime("%Y-%m-%d %H:%M:%S")} - {message}\n')
    print(message)


def start_driver(url, down_path='/Users/parkbyeongjun/datanuri/crawling'):
    """ Selenium WebDriver를 시작하고, 다운로드 경로 및 브라우저 설정 적용 """
    try:
        log('#### Start Driver')
        chrome_options = webdriver.ChromeOptions()

        # 다운로드 경로 설정 및 기타 옵션
        prefs = {
            'download.default_directory': down_path,
            'download.prompt_for_download': False,
            'download.directory_upgrade': True,
            'safebrowsing.enabled': True
        }
        chrome_options.add_experimental_option('prefs', prefs)

        # WebDriverManager를 사용하여 ChromeDriver 설치 및 실행
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.implicitly_wait(10)  # 암묵적 대기 시간 설정
        driver.get(url)  # 주어진 URL로 이동

        log('#### Driver started successfully')
        return driver
    except Exception as e:
        log('######## Start Driver Error')
        log(traceback.format_exc())
        return None  # 드라이버 시작 실패 시 None 반환


def collect_data(driver, dgk_df, page_num_start, page_num):
    """ 데이터 수집 및 메타데이터 처리 함수 """
    for i in range(1, 11):  # 각 페이지에서 10개의 데이터 수집
        idx = (10 * (page_num - page_num_start)) + i - 1
        title = ''

        try:
            # 데이터 클릭
            data_click_xpath = f'//*[@id="fileDataList"]/div[2]/ul/li[{i}]/dl/dt/a'
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, data_click_xpath))).click()

            # 데이터 title 가져오기
            title_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="contents"]/div[2]/div[1]/div[1]/p')))
            title = title_element.text
            dgk_df.loc[idx, 'title'] = title
            log(f'#### # {idx} Collect : \'{title}\'')
            time.sleep(1)

            # 다운로드 버튼 클릭
            try:
                download_button_xpath = '//*[@id="tab-layer-file"]/div[2]/div[2]/a'
                WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, download_button_xpath))).click()
            except:
                log(f'####### No file found for: \'{title}\'')

            # Alert 처리
            try:
                WebDriverWait(driver, 3).until(EC.alert_is_present())
                alert = driver.switch_to.alert
                alert.accept()
                time.sleep(1)
            except:
                log(f'####### No alert for: \'{title}\'')

        except Exception as e:
            log(f'#### Click Error: \'{title}\'')
            log(traceback.format_exc())
            continue  # 오류가 발생하면 다음 데이터로 넘어감

        try:
            # 메타데이터 수집
            url = driver.current_url
            response = requests.get(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            periodicity = soup.find_all('td', 'td custom-cell-border-bottom')[7].text
            dgk_df.loc[idx, 'periodicity'] = periodicity

            # JSON 메타데이터 수집
            url_schema = re.sub('.do', '.json', re.sub('/data/', '/catalog/', url))
            response = requests.get(url_schema)
            soup_dict = response.json()

            # 메타데이터 저장
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
            dgk_df.loc[idx, 'creator_telephone'] = soup_dict.get('creator', {}).get('contactPoint', {}).get(
                'telephone', '').replace('+82-', '')
            dgk_df.loc[idx, 'distribution_encodingFormat'] = soup_dict.get('distribution', [{}])[0].get(
                'encodingFormat', '').lower()
            dgk_df.loc[idx, 'distribution_contentUrl'] = soup_dict.get('distribution', [{}])[0].get('contentUrl', '')

            log('############ Collect Complete!')
            driver.back()

            # 데이터 저장 (100개마다)
            if idx % 100 == 0:
                save_path = f'{filePath}/dgk_data_{page_num_start}_{page_num}.csv'
                dgk_df.to_csv(save_path, index=False, encoding='UTF-8-SIG')
                log(f'#### To CSV: {save_path}')

        except Exception as e:
            log(f'######## Collect Error: \'{title}\'')
            log(traceback.format_exc())
            driver.back()


def main():
    """ 메인 실행 함수 """
    page_num_start = 1  # 시작 페이지
    page_num_end = 10  # 끝 페이지
    url_template = 'https://www.data.go.kr/tcs/dss/selectDataSetList.do?dType=FILE&keyword=&currentPage={}&perPage=10'

    driver = start_driver(url_template.format(page_num_start))

    if driver is None:
        log('#### Driver failed to start. Exiting program.')
        return

    driver.maximize_window()

    # 데이터프레임 생성
    dgk_df = pd.DataFrame(columns=[
        'title', 'periodicity', 'description', 'url', 'keywords', 'license',
        'dateCreated', 'dateModified', 'datePublished', 'creator_name',
        'creator_contactType', 'creator_telephone', 'distribution_encodingFormat',
        'distribution_contentUrl'
    ])

    # 페이지 순회 및 데이터 수집
    for page_num in range(page_num_start, page_num_end + 1):
        log(f'#### Page Number: {page_num}')
        collect_data(driver, dgk_df, page_num_start, page_num)

        # 다음 페이지로 이동
        try:
            next_button_xpath = f'/html/body/div[2]/div/div/div/div[10]/nav/a[{page_num % 10 + 2}]'
            WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, next_button_xpath))).click()
            log('#### Next Page')
        except Exception as e:
            log(f'#### Failed to navigate to next page: {page_num}')
            log(traceback.format_exc())
            break  # 다음 페이지 이동 실패 시 중단

    # 최종 데이터 저장
    save_path = f'{filePath}/dgk_data_{page_num_start}_{page_num_end}.csv'
    dgk_df.to_csv(save_path, index=False, encoding='UTF-8-SIG')
    log(f'#### Final CSV saved at: {save_path}')

    driver.quit()
    log('#### Driver closed successfully')


# 프로그램 실행
if __name__ == '__main__':
    start_time = time.time()

    main()

    log('#### ===================== Time =====================')
    log(f'#### {time.time() - start_time:.3f} seconds')
    log('#### ================================================')
