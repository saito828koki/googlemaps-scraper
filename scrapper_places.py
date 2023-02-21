from googlemaps import GoogleMapsScraper

if __name__ == "__main__":
    scraper = GoogleMapsScraper(debug=True)

    scraper.get_places(method="squares", keyword_list=["laser"])
