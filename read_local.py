import datetime
import logging
import sys
from argparse import ArgumentParser, ArgumentTypeError

from m4m_sync.encrypt import AesStreamWrapper
from m4m_sync.stores import LocalStore, Sensor
from m4m_sync.utils import DateTimeRange

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)

logger = logging.getLogger(__name__)


def valid_date(s):
    try:
        return datetime.datetime.strptime(s, "%d-%m-%Y")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise ArgumentTypeError(msg)

def main():
    parser = ArgumentParser()
    parser.add_argument("--serializer", default="CsvRawSerializer")
    parser.add_argument("--key", required=False, type=str)
    parser.add_argument("--root", required=True)
    parser.add_argument("--sensor", type=str, required=True)
    parser.add_argument("--controller", type=str, required=True)
    parser.add_argument("--date", type=valid_date, required=True)
    parser.add_argument("--output", "-o", default="out.tsv")

    args = parser.parse_args()

    logger.info("init")

    store = LocalStore(

        root=args.root,
    )

    for data in store.get(
            sensor=Sensor(id=args.sensor, controller=args.controller),
            range=DateTimeRange.day(args.date),
            stream_wrapper=AesStreamWrapper(key=args.key.encode("utf-8")) if args.key else None,
    ):
        with open(args.output, "wb") as file:
            file.write(data)

    logger.info("done")


if __name__ == "__main__":
    main()
