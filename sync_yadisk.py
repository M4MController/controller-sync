import logging
import sys
from argparse import ArgumentParser

from database import DatabaseManager
from serializers import CsvSerializer
from synchronizers import YaDiskSynchronizer

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    parser = ArgumentParser()
    parser.add_argument("--db-uri", required=True)
    parser.add_argument("--token", required=True)

    args = parser.parse_args()

    logger.info("init")

    db = DatabaseManager(args.db_uri)
    serializer = YaDiskSynchronizer(
        database=db,
        serializer=CsvSerializer(),
        token=args.token,
    )
    serializer.sync()

    logger.info("done")


if __name__ == "__main__":
    main()
