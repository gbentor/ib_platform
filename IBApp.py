import logging
import os

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

from IBUtils import get_req_id, get_opt_arr_from_line, get_arr_from_line, Config
from collections import OrderedDict
import datetime as dt
from random import randint
import time
from abc import ABC, abstractmethod
import threading
from collections import defaultdict
from Utils import get_closest_expiry, take_closest

OPEN_SPOT_PRICE_REQ_ID = 1
ALL_OPTION_CONTRACTS_DETAILS_REQ_ID = 2
SUPPORTED_SEC_TYPES = ['OPT', 'STK', 'FX']

logging.getLogger("IBLog")
logging.basicConfig(format='%(message)s')
logging.getLogger("IBLog").setLevel(logging.INFO)


class DataRequest:
    """
    DataRequest holds all the necessary information needed to send a new data request
    """
    __slots__ = ['contract', 'query_time', 'interval_size', 'bid_or_ask', 'req_id']

    def __init__(self, contract: Contract, query_time: dt, interval_size: int, bid_or_ask: str):
        """
        create new DataRequest object
        :param contract: contract of the request
        :param query_time: start time of the request
        :param interval_size: time span of the request (i.e. 30 minutes, 60 minutes, etc)
        :param bid_or_ask: bid side or ask side request
        """
        self.contract = contract
        self.query_time = query_time
        self.interval_size = interval_size
        self.bid_or_ask = bid_or_ask
        self.req_id = -1


class option_chain_data:
    """
    When handling options data, we keep the entire options chain, with the contract for each specific option
    """
    __slots__ = ['underline', 'open_spot_price', 'all_contracts', 'all_contracts_fetched']

    def __init__(self, underline: str):
        """
        :param underline: the underline asset of the options
        """
        self.underline = underline
        self.open_spot_price = -1
        self.all_contracts = defaultdict(lambda: {})
        self.all_contracts_fetched = False

    def __del__(self):
        self.all_contracts = {}


class IBapi(EWrapper, EClient, ABC):
    """
    Abstract class that handles all communication with TWS.
    """
    def __init__(self, output_type):
        EClient.__init__(self, self)
        self.output_type = output_type
        self.output_file = None
        self.option_chain_data = None
        self.contracts_to_delete = defaultdict(lambda: [])
        self.req_id_to_contract = {}
        self.open_requests = 0
        self.sent_time_queue = []
        self.mode = "historical"
        self.lock = threading.Lock()

        self.__shift_hours = 0

    @abstractmethod
    def create(self, output_type: str):
        pass

    @abstractmethod
    def historicalData(self, req_id: int, bar):
        """
        The answer from the IB server, with the requested data.
        Each inheriting class has it's own implementation
        :param req_id: the id of the request
        :param bar: the bar object, containing date, open, low, high and close
        """
        update_time = dt.datetime.strptime(bar.date, '%Y%m%d  %H:%M:%S')
        update_time = update_time.replace(hour=update_time.hour - self.__shift_hours)  # shift time if needed
        bar.date = update_time.strftime('%H%M%S')

    def send_live_data_request(self, contract):
        pass

    def send_historical_data_request(self, data_request: DataRequest):
        """
        add new request to sent_time_queue and increase the open requests counter.
        further implementation is specific for each inheriting class
        :param data_request: the new request
        """
        self.sent_time_queue.append(dt.datetime.now())
        self.open_requests += 1

    def historicalDataEnd(self, req_id: int, start: str, end: str):
        """
        Indication all the data for that request has been delivered.
        :param req_id: the request id
        :param start: start time of the request
        :param end: end time of the request
        """
        super().historicalDataEnd(req_id, start, end)
        if req_id in self.req_id_to_contract.keys():
            # calculate the time it took for the request the be delivered, and than remove it from the requests dictionary
            fetch_time = (dt.datetime.now() - self.req_id_to_contract[req_id]['time']).seconds
            data_req = self.req_id_to_contract[req_id]["data_request"]
            msg = f"HistoricalDataEnd - {req_id:05}. Strike: {data_req.contract.right}{data_req.contract.strike}, from: {start}, to: {end}, send time: {self.req_id_to_contract[req_id]['time'].strftime('%H:%M:%S')}, end time: {dt.datetime.now().strftime('%H:%M:%S')}, fetch time: {fetch_time}"
            logging.getLogger("IBLog").info(msg)
            del self.req_id_to_contract[req_id]
            self.open_requests -= 1
        elif req_id == OPEN_SPOT_PRICE_REQ_ID:
            logging.getLogger("IBLog").info(f"HistoricalDataEnd. req_id: {req_id}, from {start} to {end}")
        else:
            raise Exception("Unknown req_id!!!")

    def contractDetails(self, req_id: int, contract_details):
        """
        the answer from IB servers with the contract details needed to send data requests
        :param req_id: id of the request
        :param contract_details:
        """
        super().contractDetails(req_id, contract_details)
        if req_id == ALL_OPTION_CONTRACTS_DETAILS_REQ_ID:
            self.option_chain_data.all_contracts[contract_details.contract.strike][contract_details.contract.right] = contract_details.contract

    def contractDetailsEnd(self, req_id: int):
        """
        Indication that all contracts for this request have been delivered
        :param req_id: request id
        """
        super().contractDetailsEnd(req_id)
        if req_id == ALL_OPTION_CONTRACTS_DETAILS_REQ_ID:
            self.option_chain_data.all_contracts_fetched = True

    def error(self, req_id, error_code: int, error_string: str):
        """
        Error message coming from IB servers.
        We ignore some messages when retrieving historical data, since they don't concern us (i.e. live data feed disconnection).
        When a certain option doesn't have data, like when close to expiry, we add it to the contracts_to_delete list, so we'd
        know not to send further request for her.
        All error codes: https://interactivebrokers.github.io/tws-api/message_codes.html
        :param req_id:
        :param error_code:
        :param error_string:
        """
        if self.mode == "historical" and error_code in [2103, 2104, 2108, 2157, 2158]:
            return
        logging.getLogger("IBLog").error(f"ERROR {dt.datetime.now().strftime('%H:%M:%S.%f')} {req_id:05} {error_code} {error_string}")
        if req_id in self.req_id_to_contract.keys():
            strike = self.req_id_to_contract[req_id]["data_request"].contract.strike
            if error_code == 162 and error_string.split(':')[1] == "HMDS query returned no data":
                self.contracts_to_delete[strike].append(self.req_id_to_contract[req_id]["data_request"].contract.right)
                self.open_requests -= 1
            if error_code == 165:
                pass

    def get_all_needed_contracts(self, asset, date, is_weekly, config: Config):
        """
        Currently only OPT needs an implementation of this function
        :param asset:
        :param date:
        :param is_weekly:
        :param config:
        :return:
        """
        return

    def write_to_file(self, input_line: str):
        """
        either write to txt file, or to binary file. If the latter selected, first convert it to numpy array so it can
        be serialized.
        :param input_line:
        """
        if self.output_type == "bin":
            if isinstance(self, OPT):
                output = get_opt_arr_from_line(input_line)
            else:
                output = get_arr_from_line(input_line)
            self.lock.acquire()
            output.tofile(self.output_file)
            self.lock.release()
        elif self.output_type == "txt":
            output = input_line
            self.lock.acquire()
            self.output_file.write(output)
            self.lock.release()
        else:
            raise Exception("Unknown output file type")

    def set_shift_hours(self, shift: int):
        """
        Time stamp of data depends of the AWS timezone.
        Set this values to anything but 0, to shift the time before writing to file
        :param shift: shift time in hours
        """
        self.__shift_hours = shift

    def remove_contracts(self):
        """
        Remove contracts that we received no data error for them
        """
        for strike in self.contracts_to_delete.keys():
            if strike in self.option_chain_data.all_contracts.keys():
                for side in self.contracts_to_delete[strike]:
                    if side in self.option_chain_data.all_contracts[strike]:
                        del self.option_chain_data.all_contracts[strike][side]
        self.contracts_to_delete.clear()

    @staticmethod
    def get_asset_contract(symbol):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = 'STK'
        contract.exchange = 'SMART'
        contract.currency = 'USD'

        return contract


class OPT(IBapi):
    def __init__(self, output_type: str):
        super().__init__(output_type)

    def create(self, output_type: str):
        return OPT(output_type)

    def send_historical_data_request(self, data_request: DataRequest):
        """
        Generate a new random request id and send a new request
        :param data_request: request object with all needed data
        :return:
        """
        super().send_historical_data_request(data_request)

        data_request.req_id = get_req_id(self.req_id_to_contract.keys())

        self.req_id_to_contract[data_request.req_id] = {"secType": "OPT", "data_request": data_request, "time": dt.datetime.now()}

        self.reqHistoricalData(int(data_request.req_id), data_request.contract, f"{data_request.query_time.strftime('%Y%m%d %H:%M:%S')} EST", f"{data_request.interval_size * 60} S", "5 secs", data_request.bid_or_ask, 1, 1, False, [])

    def historicalData(self, req_id: int, bar):
        """
        Response from IB servers.
        We parse the incoming data and making it ready for writing to file.
        :param req_id: request id
        :param bar: data
        """
        super().historicalData(req_id, bar)
        if req_id == OPEN_SPOT_PRICE_REQ_ID:
            # If the request was to get the open spot price, we only want to save it in order to create the option chain
            # no need to save this data to file
            self.option_chain_data.open_spot_price = bar.close
            logging.getLogger("IBLog").info(f"Open price is: {str(bar.close)}")
            return

        if req_id in self.req_id_to_contract.keys():
            data_request = self.req_id_to_contract[req_id]["data_request"]
            strike = data_request.contract.strike
            call_or_put = data_request.contract.right
            bid_or_ask = "S" if data_request.bid_or_ask == "ASK" else "B"
            string = f"{bar.date},{strike},{call_or_put},{bid_or_ask},{format(bar.open, '.3f')},{format(bar.high, '.3f')},{format(bar.low, '.3f')},{format(bar.close, '.3f')}\n"
        else:
            raise Exception("Unknown req_id!!!")
        self.write_to_file(string)

    def get_wanted_contracts(self, asset: str):
        """
        Get a list of the contracts corresponding with the options
        :return: list of all contracts
        """
        all_contracts = []
        for strike in self.option_chain_data.all_contracts.values():
            for side in strike:
                all_contracts.append(strike[side])

        return all_contracts

    def get_all_needed_contracts(self, asset: str, date: dt, is_weekly: bool, config: dict):
        """
        Each strike on each side is considered a different contract, So when requesting data on options we first need
        to decide with strikes on each side we want get.
        :param asset: the underline asset
        :param date: requested date
        :param is_weekly: weekly options or monthly
        :param config: config params
        """
        self.option_chain_data = option_chain_data(asset)
        self.contracts_to_delete.clear()

        next_expiry = get_closest_expiry(date, os.getcwd(), is_weekly)  # get the closest expiry to this dates, depending if that's a monthly or weekly option
        logging.getLogger("IBLog").info(f"Expiry found for date {date.strftime('%d/%m/%Y')}: {next_expiry.strftime('%d/%m/%Y')}")

        self.get_underline_open_price(asset, date.replace(hour=config.start_time.hour, minute=config.start_time.minute))

        # Get all contracts for the given expiry
        self.reqContractDetails(ALL_OPTION_CONTRACTS_DETAILS_REQ_ID, self.get_ambiguous_option_contract(next_expiry, asset))
        while not self.option_chain_data.all_contracts_fetched:
            time.sleep(1)

        self.keep_close_strikes(config.pct_strikes_from_atm)

        [logging.getLogger("IBLog").info(contract) for contract in self.option_chain_data.all_contracts.values()]

    def keep_close_strikes(self, dist_from_atm: float):
        """
        Delete all strikes that are too far away from ATM
        :param dist_from_atm: percentage from ATM. keep all strikes that are within this range
        """
        self.option_chain_data.all_contracts = OrderedDict(sorted(self.option_chain_data.all_contracts.items()))
        atm_strike = take_closest(list(self.option_chain_data.all_contracts.keys()), self.option_chain_data.open_spot_price)
        for strike in self.option_chain_data.all_contracts.keys():
            if abs(strike - atm_strike) / atm_strike > dist_from_atm:
                self.contracts_to_delete[strike] = []  # we are going to delete both Call and Put, so we don't specify
        for strike in self.contracts_to_delete:
            del self.option_chain_data.all_contracts[strike]
        self.contracts_to_delete.clear()

    def get_underline_open_price(self, asset: str, date: dt):
        """
        Since we are interested in strikes around ATM, we first need to know what is underline price at the beginning
        of the day
        :param asset: the underline asset
        :param date: requested date
        """
        underline_contract = self.get_asset_contract(asset)
        is_live_data_request = date.date() == dt.datetime.now().date()
        if not is_live_data_request:
            self.reqHistoricalData(OPEN_SPOT_PRICE_REQ_ID, underline_contract, date.strftime("%Y%m%d %H:%M:%S") + " EST", "60 S", "1 min", "BID_ASK", 1, 1, False, [])
        else:
            self.reqMktData(OPEN_SPOT_PRICE_REQ_ID, underline_contract, "", False, False, [])
        while self.option_chain_data.open_spot_price == -1:  # wait until spot price is initialized
            time.sleep(1)
        if is_live_data_request:
            self.cancelMktData(OPEN_SPOT_PRICE_REQ_ID)

    @staticmethod
    def get_ambiguous_option_contract(next_expiry: dt, underline_asset: str) -> Contract:
        """
        Ambiguous contract for options gives all the contracts on all all strikes for a requested underline and expiry
        :param next_expiry: exipry of the option series
        :param underline_asset: asset
        :return: ambiguous contract
        """
        contract = Contract()
        contract.symbol = underline_asset
        contract.secType = "OPT"
        contract.exchange = "SMART"
        contract.currency = "USD"
        contract.strike = 0
        contract.lastTradeDateOrContractMonth = next_expiry.strftime("%Y%m%d")

        return contract


class STK(IBapi):
    def __init__(self, output_type: str):
        super().__init__(output_type)

    def create(self, output_type: str):
        return STK(output_type)

    def send_historical_data_request(self, data_request: DataRequest):
        """
        Generate a new random request id and send a new request
        :param data_request: request object with all needed data
        """
        super().send_historical_data_request(data_request)

        data_request.req_id = get_req_id(self.req_id_to_contract.keys())
        self.req_id_to_contract[data_request.req_id] = {"secType": "STK", "data_request": data_request, "time": dt.datetime.now()}
        self.reqHistoricalData(data_request.req_id, data_request.contract,  f"{data_request.query_time.strftime('%Y%m%d %H:%M:%S')} EST", f"{data_request.interval_size * 60} S", "5 secs", data_request.bid_or_ask, 1, 1, False, [])

    def historicalData(self, req_id: int, bar):
        """
        Response from IB servers.
        We parse the incoming data and making it ready for writing to file.
        :param req_id: request id
        :param bar: data
        """
        super().historicalData(req_id, bar)
        if req_id in self.req_id_to_contract.keys():
            data_request = self.req_id_to_contract[req_id]["data_request"]
            bid_or_ask = "S" if data_request.bid_or_ask == "ASK" else "B"
            string = f"{bar.date},{bid_or_ask},{format(bar.open, '.3f')},{format(bar.high, '.3f')},{format(bar.low, '.3f')},{format(bar.close, '.3f')}\n"
        else:
            raise Exception("Unknown req_id!!!")
        self.write_to_file(string)

    def get_wanted_contracts(self, asset: str):
        """
        STK only has single contract per asset
        :param asset: requested asset
        """
        return [self.get_asset_contract(asset)]


class FX(IBapi):
    def __init__(self, output_type: str):
        super().__init__(output_type)

    def create(self, output_type: str):
        return FX(output_type)

    def send_historical_data_request(self, data_request: DataRequest):
        """
        Generate a new random request id and send a new request
        :param data_request: request object with all needed data
        """
        req_id = randint(10, 99999)
        while req_id in self.req_id_to_contract.keys():
            req_id = randint(10, 99999)
        self.req_id_to_contract[req_id] = {"secType": "FX", "strike": 0, "bidOrAsk": data_request.bid_or_ask, "callOrPut": "F", "time": dt.datetime.now()}

        self.reqHistoricalData(int(req_id), data_request.contract,  f"{data_request.query_time.strftime('%Y%m%d %H:%M:%S')} EST", f"{data_request.interval_size * 60} S", "5 secs", data_request.bid_or_ask, 1, 1, False, [])
        self.sent_time_queue.append(dt.datetime.now())
        self.open_requests += 1
        time.sleep(0.1)

    def historicalData(self, req_id: int, bar):
        """
        Response from IB servers.
        We parse the incoming data and making it ready for writing to file.
        :param req_id: request id
        :param bar: data
        """
        super().historicalData(req_id, bar)
        if req_id in self.req_id_to_contract.keys():
            data_request = self.req_id_to_contract[req_id]["data_request"]
            bid_or_ask = "S" if data_request.bid_or_ask == "ASK" else "B"
            string = f"{bar.date},{bid_or_ask},{format(bar.open, '.3f')},{format(bar.high, '.3f')},{format(bar.low, '.3f')},{format(bar.close, '.3f')}\n"
        else:
            raise Exception("Unknown req_id!!!")
        self.write_to_file(string)

    def get_wanted_contracts(self, asset: str):
        """
        STK only has single contract per asset
        :param asset: requested asset
        """
        return [self.get_fx_contract(asset)]

    @staticmethod
    def get_fx_contract(symbol):
        contract = Contract()
        contract.symbol = symbol.split('.')[0].upper()
        contract.secType = "CASH"
        contract.currency = symbol.split('.')[1].upper()
        contract.exchange = "IDEALPRO"

        return contract


class IBFactory(object):
    """
    Factory function to create the required instance
    """
    @classmethod
    def createIBapi(cls, designation, output_type):
        if designation not in [cls.__name__ for cls in IBapi.__subclasses__()]:
            raise Exception("Unrecognized class")
        return eval(designation)(output_type)
