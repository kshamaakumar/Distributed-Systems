"""
CPSC 5520, Seattle University
This is free and unencumbered software released into the public domain.
:Authors: Kevin Lundeen
:Version: f19-02

This module is the subscriber-side functions for marshalling message components analogous to the publisher-side
functions supplied by Forex Provider in fxp_bytes.py.
"""
import ipaddress
from array import array
from datetime import datetime

RECORD_LENGTH = 32  # bytes


def deserialize_price(b: bytes) -> float:
    """
    Convert an 8-byte section of a Forex Provider message as a floating-point number.
    A 64-bit floating point number represented in IEEE 754 binary64 little-endian format.

    >>> deserialize_price(b'\\x05\\x04\\x03\\x02\\x01\\xff?C')  # extra backslashes are because we are in a docstring here
    9006104071832581.0

    :param b:  8-byte byte stream from Forex Provider
    :return: the number
    """
    a = array('d')
    a.frombytes(b)
    return a[0]


def deserialize_utcdatetime(b: bytes) -> datetime:
    """
    Inverse of serialize_utcdatetime.

    >>> deserialize_utcdatetime(b'\\x00\\x007\\xa3e\\x8e\\xf2\\xc0')
    datetime.datetime(1971, 12, 10, 1, 2, 3, 64000)

    :param b: 8-byte stream
    :return: utc datetime
    """
    a = array('Q')
    a.frombytes(b)
    a.byteswap()
    micros = a[0]
    seconds = micros // 1_000_000
    fractional = micros % 1_000_000
    approx = datetime.utcfromtimestamp(seconds)
    return datetime(approx.year, approx.month, approx.day, approx.hour, approx.minute, approx.second, fractional)


def serialize_address(host_ip: str, port: int) -> bytes:
    """
    Get the byte-stream in network format.

    >>> serialize_address('127.0.0.1', 65534)
    b'\\x7f\\x00\\x00\\x01\\xff\\xfe'

    :param host_ip: ip address string of the host to send UDP packets to (numeric -- no DNS lookup here)
    :param port: port number where the UPD packets should go on host
    :return: 6-byte sequence suitable to to be sent to Forex Provider subscription service enrollment
    """
    ip = ipaddress.ip_address(host_ip)
    if ip.version != 4:
        raise ValueError('Forex Provider only supports IPv4')
    p = array('H', [port])
    p.byteswap()  # make big-endian
    return ip.packed + p.tobytes()  # ip.packed is already big-endian, as required


def unmarshal_message(message: bytes):
    """
    Decode message data from a published message by Forex Providers.

    >>> message = b'\\x00\\x04\\tT\\xdd5@\\x00GBPUSD\\xbba\\xdb\\xa2\\xcc\\x86\\xf3?'
    >>> message += b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'
    >>> message += b'\\x00\\x04\\t@\\xbf]\\xe0\\x00USDJPY\\x12\\x83\\xc0\\xca\\xa1\\x11[@'
    >>> message += b'\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00'
    >>> records = unmarshal_message(message)
    >>> len(records)
    2
    >>> records[0]
    (datetime.datetime(2006, 1, 2, 0, 0), 'GBP', 'USD', 1.22041)
    >>> records[1]
    (datetime.datetime(2006, 1, 1, 0, 0), 'USD', 'JPY', 108.2755)

    :param message: UDP datagram contents from Forex Providers published message
    :return: list of tuples: (timestamp, ccy1, ccy2, exchange_rate)
    """
    records = []
    for i in range(0, len(message), RECORD_LENGTH):
        record = message[i:i + RECORD_LENGTH]
        ts = deserialize_utcdatetime(record[:8])
        ccy1 = record[8:11].decode('utf-8')
        ccy2 = record[11:14].decode('utf-8')
        price = deserialize_price(record[14:22])
        records.append((ts, ccy1, ccy2, price))
    return records
