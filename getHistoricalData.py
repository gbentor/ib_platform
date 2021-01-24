from IBApp import DataRequest, IBFactory
from IBUtils import init_app_listener, is_weekly_options, Config

import time
import os
import tkinter as tk
from datetime import datetime, timedelta
from tkinter import messagebox
import logging

logging.getLogger("MainLogger")
logging.basicConfig(format='%(message)s')
logging.getLogger("MainLogger").setLevel(logging.INFO)


def check_pacing_violations(app):
    """
    Only 60 data request are allowed in 10 minutes, and 50 request may be active at the same time.
    We need to check there are no violations of these limitations before sending a new data request.
    :param app: the current IBapi object
    """
    while not app.isConnected():
        time.sleep(1)

    # check if the first request in the queue was sent more than 10 minutes ago. if so, remove it from queue.
    while len(app.sent_time_queue) > 58:
        if (datetime.now() - app.sent_time_queue[0]).seconds / 60 > 10:
            app.sent_time_queue.pop(0)
        time.sleep(0.01)

    # check that there are no more than 50 open requests
    while app.open_requests > 49:
        time.sleep(0.01)


def is_file_exists(filepath: str) -> bool:
    """
    If file already exists, ask whether to overwrite or not.
    :param filepath:
    :return: should overwrite (if exists) or not
    """
    if os.path.exists(filepath):
        root = tk.Tk()
        answer = messagebox.askquestion('File already exists', 'File already exists!\nOverwrite?', icon='warning')
        root.destroy()
        if answer != 'yes':
            return True
    return False


def get_times_and_interval(start_time: datetime, end_time: datetime, date: datetime, request_interval: int) -> (datetime, int, datetime):
    """
    The first request might have a shorter span since it's not guaranteed the requested interval will fit completely to
    the day. for example trading in the US starts at 9:30 and ends at 16:00 so if we want a 60 minutes intervals, the first
    one will be only 30 minutes.
    :param start_time: start of trading time
    :param end_time: end of trading time
    :param date: requested date
    :param request_interval: interval
    :return:
    """
    tmp = date.replace(hour=end_time.hour, minute=end_time.minute)
    start_time = date.replace(hour=start_time.hour, minute=start_time.minute)
    while tmp - timedelta(minutes=request_interval) > start_time:
        tmp -= timedelta(minutes=request_interval)
    query_time = tmp
    end_time = date.replace(hour=end_time.hour, minute=end_time.minute) + timedelta(minutes=5)
    interval_size = (query_time - start_time).seconds / 60
    return end_time, int(interval_size), query_time


def main(config: Config):
    app = IBFactory.createIBapi(config.sec_type, config.output_type)
    app.set_shift_hours(config.shift_hours)

    logging.getLogger("MainLogger").info(''.join(["Dates: "] + [date.strftime('%d/%m/%Y') + ", " for date in config.dates] + ["\n"]))

    init_app_listener(app)

    for asset in config.assets:
        base_asset, is_weekly = is_weekly_options(asset)  # decide if that's weekly options or not

        # set output directory for files
        directory = os.path.join(config.output_dir, base_asset)
        directory = f"{directory}_OPTIONS" if config.sec_type == 'OPT' else directory
        os.makedirs(directory, exist_ok=True)

        for date in config.dates:
            start_timer = datetime.now()
            logging.getLogger("MainLogger").info(f"{asset} - {date.strftime('%Y%m%d')}")

            app.get_all_needed_contracts(base_asset, date, is_weekly, config)

            end_time, interval_size, query_time = get_times_and_interval(config.start_time, config.end_time, date, config.request_interval)

            file_name_ending = f"OPTION-{query_time.date()}" if config.sec_type == 'OPT' else f"{query_time.date()}"
            file_name = os.path.join(directory, f"RawData-{asset}-{file_name_ending}.{config.output_type}")

            if is_file_exists(file_name):
                # if file already exists and we don't want to overwrite - continue to next date
                continue
            app.output_file = open(file_name, f"{'w+' if config.output_type == 'txt' else 'wb+'}")

            while query_time < end_time:
                app.remove_contracts()

                contracts_to_get = app.get_wanted_contracts(asset)
                for contract in contracts_to_get:
                    check_pacing_violations(app)
                    app.send_historical_data_request(DataRequest(contract, query_time, interval_size, 'ASK'))

                    check_pacing_violations(app)
                    app.send_historical_data_request(DataRequest(contract, query_time, interval_size, 'BID'))

                interval_size = config.request_interval
                query_time += timedelta(minutes=interval_size)
            while app.open_requests > 0:
                time.sleep(0.1)
            app.output_file.close()
            logging.getLogger("MainLogger").info(f"Process time of day - {divmod((datetime.now() - start_timer).total_seconds(), 60)}")

    while app.open_requests > 0:
        time.sleep(0.1)
    logging.getLogger("MainLogger").info("Terminating...")
    app.done = True
    exit()


if __name__ == "__main__":
    main(Config("config.ini"))
