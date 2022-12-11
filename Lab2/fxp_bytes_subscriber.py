import struct
from datetime import datetime, timedelta
from typing import List

MAX_QUOTES_PER_MESSAGE = 50
MICROS_PER_SECOND = 1_000_000
MESSAGE_LENGTH = 32

def deserialize_price(x: bytes) -> float: 
    p = struct.unpack('<d', x)
    return p[0]


def serialize_address(host: str, port: int) -> bytes:
    ip = bytes(map(int, host.split('.')))
    p = struct.pack('>H', port)
    return ip + p


def deserialize_utcdatetime(utc: bytes) -> datetime:
    epoch = datetime(1970, 1, 1)
    p = struct.unpack('>Q', utc)[0]
    p = p / MICROS_PER_SECOND
    return epoch + timedelta(seconds=p)


def unmarshal_message(byte_sequence: bytes) -> List[str]:
    messageArray = []  
    length = len(byte_sequence) / 32

    for x in range(int(length)):
        message = ''
        timestamp = deserialize_utcdatetime(byte_sequence[
                                            (0 + (x * MESSAGE_LENGTH)):(8 + (
                                                        x * MESSAGE_LENGTH))])
        currencyName = byte_sequence[(8 + (x * MESSAGE_LENGTH)):(
                    14 + (x * MESSAGE_LENGTH))].decode("utf-8")
        price = deserialize_price(byte_sequence[(14 + (x * MESSAGE_LENGTH)):(
                    22 + (x * MESSAGE_LENGTH))])

        message += str(timestamp) + ' '
        message += str(currencyName[0:3]) + ' '
        message += str(currencyName[3:]) + ' '
        message += str(price)
        messageArray.append(message)
    return messageArray