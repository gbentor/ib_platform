import time
import threading
from random import randint
import numpy as np
from datetime import datetime, timedelta
import configparser


class Config:
    def __init__(self, path: str):
        self.output_type = "bin"
        self.output_dir = ""
        self.sec_type = "OPT"
        self.assets = []
        self.dates = []

        # optional
        self.start_time = datetime.strptime('0930', '%H%M')
        self.end_time = datetime.strptime('1600', '%H%M')
        self.request_interval = 60
        self.pct_strikes_from_atm = 7
        self.shift_hours = 0

        self.parse_config_file(path)

    def parse_config_file(self, file_path: str):
        """
        Parse the ini config file.
        Some of the parameters are required, while the other are optional, and will be given default value if not specified
        :param file_path: file path of the config file
        :return: dictionary containing the parameters
        """
        config_parsed = configparser.ConfigParser()
        config_parsed.read(file_path)
        if 'output_type' not in config_parsed['General'].keys():
            raise Exception("Must specify output type - bin or txt")
        self.output_type = config_parsed['General']['output_type']
        if 'output_dir' not in config_parsed['General'].keys():
            raise Exception("Must specify output directory")
        self.output_dir = config_parsed['General']['output_dir']
        if 'sec_type' not in config_parsed['General'].keys():
            raise Exception("Must specify the sec_type")
        self.sec_type = config_parsed['General']['sec_type'].upper()
        if 'assets' not in config_parsed['General'].keys():
            raise Exception("Must specify the 'assets'")
        self.assets = [x.strip().upper() for x in config_parsed['General']['assets'].split(',') if len(x) > 1]
        days_to_get = config_parsed['Optional']['days_to_get'] if 'days_to_get' in config_parsed['Optional'].keys() else 1
        start_date = config_parsed['General']['start_date'] if 'start_date' in config_parsed['General'].keys() else datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
        self.get_dates_list(start_date, days_to_get)

        # Optional params
        if 'start_time' in config_parsed['Optional'].keys():
            self.start_time = datetime.strptime(config_parsed['Optional']['start_time'], '%H%M')
        if 'end_time' in config_parsed['Optional'].keys():
            self.end_time = datetime.strptime(config_parsed['Optional']['end_time'], '%H%M')
        if 'shift_hours' in config_parsed['Optional'].keys():
            self.shift_hours = int(config_parsed['Optional']['shift_hours'])
        if 'request_interval' in config_parsed['Optional'].keys():
            self.request_interval = int(config_parsed['Optional']['request_interval'])
        if 'pct_strikes_from_atm' in config_parsed['Optional'].keys():
            self.pct_strikes_from_atm = float(config_parsed['Optional']['pct_strikes_from_atm']) / 100

    def get_dates_list(self, start_date: str, days_to_get: int):
        """
        Get list of dates, based on a start date and number of days to get.
        Function excludes all weekend dates (Saturday and Sunday).
        :param start_date: string of the required date, as 'YYYYmmdd'
        :param days_to_get: number of days to look back when taking the dates
        """
        dates = start_date.split(',')
        if len(dates) == 1:
            start_date = datetime.strptime(start_date.strip(), '%Y%m%d')
            days_to_get = int(days_to_get)
            date_list = [start_date - timedelta(days=x) for x in range(days_to_get)]
        elif len(dates) > 1:
            date_list = [datetime.strptime(date.strip(), '%Y%m%d') for date in dates]
        else:
            raise Exception("No date provided")

        self.dates = [x for x in date_list if x.weekday() < 5]


def run_loop(app):
    app.run()


def init_app_listener(app):
    """
    Initiate connection with TWS
    :param app: current application
    """
    app.connect('127.0.0.1', 7496, randint(100, 999))
    time.sleep(5)
    # Start the socket in a thread
    api_thread = threading.Thread(target=run_loop, args=[app])
    api_thread.setDaemon(True)
    api_thread.start()


def get_req_id(used_ids):
    """
    Generate a random id, and check it doesn't already in use
    :param used_ids: currently used ids
    :return: new id
    """
    while (req_id := randint(10, 99999)) in used_ids:
        continue

    return req_id


def get_opt_arr_from_line(line: str):
    fields = line.split(',')
    fields[-1] = fields[-1].strip('\n')
    fields[2] = 1 if fields[2] == 'C' else 0  # Call is represented as 1, Put as 0
    fields[3] = 1 if fields[3] == 'S' else 0  # Ask is represented as 1, Bid as 0
    return np.array([np.float32(fields[0]), np.float32(fields[1]), np.float32(fields[2]), np.float32(fields[3]), np.float32(fields[4]), np.float32(fields[5]), np.float32(fields[6]), np.float32(fields[7])])


def get_arr_from_line(line: str) -> np.array:
    fields = line.split(',')
    fields[1] = 1 if fields[1] == 'S' else 0  # Ask is represented as 1, Bid as 0
    fields[-1] = fields[-1].strip('\n')
    return np.array([np.float32(fields[0]), np.float32(fields[1]), np.float32(fields[2]), np.float32(fields[3]), np.float32(fields[4]), np.float32(fields[5])])


def is_weekly_options(asset: str) -> (str, bool):
    """
    Weekly options should end with '_W'
    :param asset: name of asset. i.e. SPY_W for weekly options on SPY, or just 'SPY', to monthly option or ETF
    :return: the name of the asset(stripped from the '_W' ending), is weekly or not
    """
    base_asset = asset
    if is_weekly := (asset[-2:] == '_W'):
        base_asset = asset.split('_')[0]
        is_weekly = True
    return base_asset, is_weekly
