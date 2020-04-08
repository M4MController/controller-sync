import logging
import sys
from argparse import ArgumentParser

import serializers

from database import DatabaseManager
from synchronizers import LocalSynchronizer

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
    parser.add_argument("--root", required=True)

    args = parser.parse_args()

    logger.info("init")

    db = DatabaseManager(args.db_uri)
    serializer = LocalSynchronizer(database=db, serializer=getattr(serializers, args.serializer)(), root=args.root)
    serializer.sync()

    logger.info("done")


if __name__ == "__main__":
    main()
