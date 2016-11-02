# coding=utf-8
# python3
from os import mkdir
from multiprocessing import Pool
import requests

base_dir = "data/"
mkdir(base_dir)
base_url = "https://www.wunderground.com/history/airport/EGLL/{}/{}/{}/DailyHistory.html?format=1"


def fetch_page(year, month, day):
    "the funtion to fetch one page"
    data = requests.get(base_url.format(year, month, day)).text.replace("<br />", "")[1:]
    output_file = base_dir + year + month + day + ".txt"
    with open(output_file, "wt") as f:
        f.write(data)


def prepare_querys():
    "prepare query list"
    querys = [(str(y), str(m).rjust(2, "0"), str(d).rjust(2, "0")) for y in range(2009, 2016) for m in range(1, 13) for
              d in range(1, 32)]
    querys_2016 = [("2016", str(m).rjust(2, "0"), str(d).rjust(2, "0")) for m in range(1, 7) for d in range(1, 32)]
    querys.extend(querys_2016)
    return querys


def fetch_pages(cores=100):
    "fetch the dataset"
    querys = prepare_querys()
    pool = Pool(processes=cores)
    for qi in querys:
        pool.apply_async(fetch_page, args=(*qi,))
    pool.close()
    pool.join()


if __name__ == '__main__':
    fetch_pages()
    print("all done!")
