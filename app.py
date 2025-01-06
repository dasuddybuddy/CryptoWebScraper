from playwright.sync_api import sync_playwright
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

def main():
    with sync_playwright() as p:

        # scrape

        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto('https://coinmarketcap.com/')

        # scraping down
        for i in range(5):
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(1000)


        trs_xpath = '//*[@id="__next"]/div[2]/div[1]/div[2]/div/div[1]/div[5]/table/tbody/tr'
        trs_list = page.query_selector_all(trs_xpath)

        master_list = []
        
        for tr in trs_list:

            coin_dict = {}

            # Extracting td elements inside each tr
            tds = tr.query_selector_all('td')

            coin_dict['id'] = tds[1].inner_text()
            coin_dict['Name'] = tds[2].query_selector('div > a > div > div > div > p').inner_text()
            coin_dict['Symbol'] = tds[2].query_selector('div > a > div > div > div > div').inner_text()
            coin_dict['Price'] = float(tds[3].inner_text().replace('$', '').replace(',', ''))
            coin_dict['Market_cap_usd'] = int(tds[7].inner_text().replace('$', '').replace(',', ''))
            coin_dict['Volume_24h_usd'] = int(tds[8].query_selector('div > a > p').inner_text().replace('$', '').replace(',', ''))
            coin_dict['scrape_date'] = datetime.now()

            master_list.append(coin_dict)


        list_of_tuples = [tuple(dic.values()) for dic in master_list]

        


        # save

        # connect to database
        pgconn = psycopg2.connect(
            host = '', #pgadmin4 server
            database = 'postgres',
            user = '', #username
            password = '' #password of server
        )

        # create cursor
        pgcursor = pgconn.cursor()

        execute_values(pgcursor,
            """
            INSERT INTO crypto (id, name, symbol, price_usd, market_cap_usd, volume_24h_usd, scrape_date)
            VALUES %s
            ON CONFLICT (id) 
            DO UPDATE SET
                price_usd = EXCLUDED.price_usd,
                market_cap_usd = EXCLUDED.market_cap_usd,
                volume_24h_usd = EXCLUDED.volume_24h_usd,
                scrape_date = EXCLUDED.scrape_date
            """,
    list_of_tuples)

        #commit
        pgconn.commit()

        pgconn.close()

        browser.close()




if __name__ == '__main__':
    main()
