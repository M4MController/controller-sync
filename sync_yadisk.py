import logging
import sys
from argparse import ArgumentParser

from core import serializers

from core.database import DatabaseManager
from core.encrypt import AesStreamWrapper
from core.stores import YaDiskSynchronizer

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    parser = ArgumentParser()
    parser.add_argument("--db-uri", required=True)
    parser.add_argument("--serializer", default="CsvRawSerializer")

    args = parser.parse_args()

    logger.info("init")

    db = DatabaseManager(args.db_uri)
    serializer = YaDiskSynchronizer(
        database=db,
        serializer=getattr(serializers, args.serializer)(),
        stream_wrapper=AesStreamWrapper(key=db.get_encryption_key()),
        token=db.get_tokens().yandex_disk,
    )
    serializer.sync()

    logger.info("done")


if __name__ == "__main__":
    main()
