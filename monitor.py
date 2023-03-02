#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import logging
import os
import sys
import json

from pathlib import Path
from googlemaps import GoogleMapsScraper
import requests


with Path("constants.json").open(mode="r") as fin:
    constants = json.load(fin)
    REVIEW_POST_URL = constants["REVIEW_POST_URL"]


class Monitor:
    def __init__(self, url_file):
        self.names = []
        self.urls = []
        # load urls file
        with open(url_file, "r") as furl:
            for row in furl:
                name, url = row.split()
                self.names.append(name)
                self.urls.append(url)

        # logging
        self.logger = self.__get_logger()

    def scrape_gm_reviews(self):
        # init scraper and incremental add reviews
        # TO DO: pass logger as parameter to log into one single file?
        with GoogleMapsScraper() as scraper:
            for name, url in zip(self.names, self.urls):
                try:
                    status = scraper.sort_by(url, 1)  # sort by newest
                    if status == 0:
                        stop = False
                        offset = 0
                        n_new_reviews = 0
                        while not stop:
                            reviews = scraper.get_reviews(offset)
                            if not reviews:
                                break

                            for review in reviews:
                                review["placeUrl"] = url
                                review["placeName"] = name
                                stop = self.__stop(review)
                                if not stop:
                                    n_new_reviews += 1
                                else:
                                    break
                            offset += len(reviews)

                        # log total number
                        self.logger.info(
                            "{} : {} new reviews".format(url, n_new_reviews)
                        )
                    else:
                        self.logger.warning("Sorting reviews failed for {}".format(url))

                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]

                    self.logger.error(
                        "{}: {}, {}, {}".format(url, exc_type, fname, exc_tb.tb_lineno)
                    )

    def __stop(self, review: dict):
        response = requests.post(REVIEW_POST_URL, json=review)
        if response.status_code == 200:
            return response.json()["result"]
        else:
            raise Exception("Error occurred.")

    def __get_logger(self):
        # create logger
        logger = logging.getLogger("monitor")
        logger.setLevel(logging.DEBUG)
        # create console handler and set level to debug
        fh = logging.FileHandler("monitor.log")
        fh.setLevel(logging.DEBUG)
        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        # add formatter to ch
        fh.setFormatter(formatter)
        # add ch to logger
        logger.addHandler(fh)

        return logger


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor Google Maps places")
    parser.add_argument("--i", type=str, default="urls.txt", help="target URLs file")

    args = parser.parse_args()

    monitor = Monitor(args.i)

    try:
        monitor.scrape_gm_reviews()
    except Exception as e:
        monitor.logger.error("Not handled error: {}".format(e))
