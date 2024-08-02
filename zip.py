import os
import requests
import zipfile
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import psycopg2
from datetime import datetime

# Create downloads directories if they don't exist
download_dir = 'downloads'
csv_dir = 'I_MANMEET__CSV'
os.makedirs(download_dir, exist_ok=True)
os.makedirs(csv_dir, exist_ok=True)

# Configure Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")

# Path to ChromeDriver
chromedriver_path = 'C:/Selenium/chromedriver.exe'

# Initialize the WebDriver
driver = webdriver.Chrome(service=Service(executable_path=chromedriver_path), options=chrome_options)

# Headers for requests
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Database connection setup
conn = psycopg2.connect(
    host='localhost',
    database='zip',
    user='postgres',
    password='8520',
    port=5432
)
conn.autocommit = True
cur = conn.cursor()

# Create table if not exists
cur.execute('''
CREATE TABLE IF NOT EXISTS cdd (
    id SERIAL PRIMARY KEY,
    path TEXT,
    updated_date DATE,
    country TEXT
)
''')

try:
    driver.get('https://www.stats.govt.nz/large-datasets/csv-files-for-download/overseas-merchandise-trade-datasets')

    download_elements = driver.find_elements(By.XPATH,
                                             "//*[@id='main']/section/div/div/div/article/div/div[1]/article/div/div/div/ul[2]//a[contains(@href, '.zip')]")

    elements = driver.find_elements(By.XPATH,
                                    "//*[@id='main']/section/div/div/div/article/div/div[1]/article/div/div/div/ul[2]/li")

    tag = driver.find_elements(By.XPATH,
                            """//*[@id="main"]/section/div/div/div/article/div/div[1]/article/div/div/div/h2[1]""")
    for i in tag:
        print('\n',i.text,'\n')

    data = []

    for element in elements:
        text = element.text
        parts = text.split(', datasets last updated')
        if len(parts) == 2:
            path = parts[0].strip()
            updated_date_str = parts[1].strip()
            try:
                updated_date = datetime.strptime(updated_date_str, '%d %B %Y').date()
                data.append((path, updated_date))
            except ValueError as e:
                print(f"Error parsing date: {updated_date_str} - {e}")

    # print(f"Data to insert: {data}\n")

    for element in download_elements:
        zip_url = element.get_attribute('href')
        link_text = element.text
        zip_filename = os.path.join(download_dir, os.path.basename(zip_url))

        if not os.path.exists(zip_filename):
            print(f"Downloading: {link_text}\n{zip_url}")

            response = requests.get(zip_url, headers=headers)
            if response.status_code == 200:
                with open(zip_filename, 'wb') as f:
                    f.write(response.content)

                if zipfile.is_zipfile(zip_filename):
                    with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                        zip_ref.extractall(csv_dir)

                    # Delete the ZIP file
                    os.remove(zip_filename)
                    print(f"Downloaded...Extracted...Deleted... \n")
                    # print(f"{link_text}")
                #     i changed 
                else:
                    print(f"Error: {zip_filename} is not a valid ZIP file.")
                    os.remove(zip_filename)
            else:
                print(f"Failed to download {link_text}. HTTP Status Code: {response.status_code}")
        else:
            print(f"File already downloaded: {zip_filename}")

finally:
    driver.quit()

# Insert data into the database
for item in data:
    path, updated_date = item
    country = 'New Zealand'
    try:
        print(f"Inserting data: {path}, {updated_date}, {country}")
        cur.execute('''
        INSERT INTO cdd (path, updated_date, country)
        VALUES (%s, %s, %s)
        ''', (path, updated_date, country))
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        conn.rollback()

cur.close()
conn.close()
