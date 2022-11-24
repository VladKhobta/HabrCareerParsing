from bs4 import BeautifulSoup
import requests
import time
from multiprocessing import Pool
from requests.exceptions import ConnectionError, Timeout
import os


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
}
START_TIME = time.time()
PATH_FOR_FILES = "D:/Parsed/"
CSV_NAME = 'habr.csv'
proxies = []


class HabrParser:

    def __init__(self, urls):
        self.urls = urls
        self.parsed_urls = []

    # one page parsing function
    def parse(self, url):
        try:
            r = requests.get(url, headers=headers, timeout=3)
        except ConnectionError:
            print('Conn Error')
            return
        except Timeout:
            print('Timeout')
            return

        if r.status_code == 404:
            print('Not Found')
            return url

        soup = BeautifulSoup(r.text, 'lxml')
        fullname = soup.find('h1', class_='page-title__title').text
        result_string = "{}\n{}\n{}".format(url, fullname, soup).encode("utf-8")

        with open("{}habr_career_{}.txt".format(PATH_FOR_FILES, time.time() - START_TIME), "wb") as file:
            file.write(result_string)

        print(f"{time.time() - START_TIME}")

        return url

    # all url pages multiprocessing parsing function
    def run(self):
        with Pool() as p:  # default os.cpu_count() for my devise -- is 8
            self.parsed_urls = p.map(self.parse, self.urls)
        return self.parsed_urls


    def check_parsed(self):
        checked_urls = []
        for root, dirs, files in os.walk(PATH_FOR_FILES):
            for filename in files:
                with open(root + filename, 'rb') as f:
                    url = f.readline()
                    checked_urls.append(url.strip())

        with open("checked_urls.txt", "w") as f:
            for checked_url in checked_urls:
                f.write(checked_url.decode("utf-8") + "\n")

        return checked_urls


def get_urls_list(filename):
    with open(filename) as f:
        urls = []

        next(f)
        for url in f:
            urls.append(url.rstrip().replace('"', ''))  # deleting quotes and escape codes

        return urls


if __name__ == "__main__":

    urls_list = get_urls_list(CSV_NAME)
    parser = HabrParser(urls_list)

    print(START_TIME)

    passed_urls = parser.run()  # parsing and getting all successfully parsed urls

    # printing script final data
    print('End time: {}'.format(time.time() - START_TIME))
    print('All urls count:', len(passed_urls))

    passed_count = 0
    with open("passed_urls.txt", "w") as file:
        for passed_url in passed_urls:
            if passed_url:
                file.write(passed_url + "\n")
                passed_count += 1

    print(f"Passed urls count(Timeout ot Connection abandon): {passed_count}")
