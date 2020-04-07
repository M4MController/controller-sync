import logging
import sys
from argparse import ArgumentParser

from database import DatabaseManager
from serializer import CsvSerializer
from synchronizers import WebDavSynchronizer

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)

logger = logging.getLogger(__name__)


def main():
    parser = ArgumentParser()
    parser.add_argument("--db-uri", required=True)
    parser.add_argument("--webdav-uri", required=True)
    parser.add_argument("--webdav-protocol", required=False)
    parser.add_argument("--webdav-username")
    parser.add_argument("--webdav-password")

    args = parser.parse_args()

    logger.info("init")

    db = DatabaseManager(args.db_uri)
    serializer = WebDavSynchronizer(
        database=db,
        serializer=CsvSerializer(),
        uri=args.webdav_uri,
        protocol=args.webdav_protocol,
        username=args.webdav_username,
        password=args.webdav_password,
    )
    serializer.sync()

    logger.info("done")


if __name__ == "__main__":
    main()