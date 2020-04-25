import datetime
import logging
import sys
from argparse import ArgumentParser, ArgumentTypeError

from core import serializers

from core.encrypt import AesStreamWrapper
from core.stores import LocalStore
from core.utils import DateTimeRange

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
    parser.add_argument("--sensor-id", type=int, required=True)
    parser.add_argument("--date", type=valid_date, required=True)
    parser.add_argument("--output", "-o", default="out.tsv")

    args = parser.parse_args()

    logger.info("init")

    serializer = LocalStore(
        serializer=getattr(serializers, args.serializer)(),
        stream_wrapper=AesStreamWrapper(key=args.key.encode("utf-8")) if args.key else None,
        root=args.root,
    )

    for data in serializer.get(args.sensor_id, DateTimeRange.day(args.date)):
        with open(args.output, "wb") as file:
            file.write(data)

    logger.info("done")


if __name__ == "__main__":
    main()
