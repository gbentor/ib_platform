from ibapi.contract import Contract

import pytest
import IBApp
import datetime as dt


@pytest.fixture
def option():
    return IBApp.IBFactory.createIBapi("OPT")


@pytest.fixture
def contract():
    contract = Contract()
    contract.symbol = 'SPY'
    contract.secType = 'STK'
    contract.exchange = 'SMART'
    contract.currency = 'USD'

    return contract


@pytest.fixture
def data_request():
    return IBApp.DataRequest(contract, dt.datetime(year=2020, month=1, day=1), 60, 'S')


def test_tick_price():
    assert True


def test_historical_data():
    assert True


def test_historical_data_end():
    assert True


def test_contract_details():
    assert True


def test_contract_details_end():
    assert True


def test_error():
    assert True


def test_send_live_option_data_req():
    assert True


def test_get_asset_contract():
    assert True


def test_create():
    assert True


def test_send_live_data_request():
    assert True


def test_send_historical_data_request(option, data_request):
    option.send_historical_data_request(data_request)
    assert len(option.sent_time_queue) == 1 and option.open_requests == 1


def test_get_wanted_contracts():
    assert True


def test_get_all_needed_contracts():
    assert True


def test_get_ambiguous_option_contract():
    assert True
