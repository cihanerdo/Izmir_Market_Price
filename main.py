import argparse
from datetime import date, datetime
from functions.helper_function import *
from functions.logger import *



parser = argparse.ArgumentParser()
parser.add_argument("-s", "--start_date", default="2023-01-01", help="Enter the Start Date (format: YYYY-MM-DD)")
parser.add_argument("-e", "--end_date", default="today", help="Enter the End Date (format: YYYY-MM-DD, or 'today' for today)")
parser.add_argument("-t", "--today", default="today", help="Enter the Date (format: YYYY-MM-DD, or 'today' for today)")


def fetch_data_upload_postgre():

    engine = create_postgresql_connection(DB_USERNAME, DB_PASSWORD, DB_HOST_IP, DB_NAME)
    args = parser.parse_args()

    start_date_str = args.start_date
    end_date_str = args.end_date

    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    except ValueError:
        print("Warning: Incorrect start date format. Please use the format YYYY-MM-DD.")
        return

    if end_date_str.lower() == "today":
        end_date = date.today()
    else:
        try:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d").date()
        except ValueError:
            print("Warning: Incorrect end date format. Please use the format YYYY-MM-DD or 'today' for today.")
            return

    logger.debug("Fetch_Data Process Begins.")
    df = fetch_data(base_url, start_date, end_date)
    df.to_csv("outputs/hal_fiyatları.csv", mode="a", index=False)
    logger.debug("Transferring to Postgres.")
    df.to_sql(name="Izmir_Market_Price", schema="stg", con=engine, index=False, if_exists='append')

# def fetch_data_upload_postgre_today():
#     engine = create_postgresql_connection(DB_USERNAME, DB_PASSWORD, DB_HOST_IP, DB_NAME)
#     args = parser.parse_args()
#
#     today = date.today()
#
#     logger.debug("Fetch_Data Process Begins..")
#     df = fetch_data_today(base_url, today)
#     logger.debug("Creating csv File.")
#     df.to_csv(f"ooutputs/hal_fiyatları_{today}.csv")
#     logger.debug("Transferring to Postgres.")
#     df.to_sql(name="Hal_Fiyatları", schema="stg", con=engine, index=False, if_exists='append')


if __name__ == "__main__":
    fetch_data_upload_postgre()
    # fetch_data_upload_postgre_today()