import logging
import sys
from argparse import ArgumentParser

from m4m_sync import serializers

from m4m_sync.database import DatabaseManager
from m4m_sync.encrypt import AesStreamWrapper
from m4m_sync.stores import LocalStore, Sensor, Controller

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
    store = LocalStore(root=args.root)
    for controller in db.get_controllers():
        c = Controller(name=controller.name, mac=controller.mac)
        store.prepare_for_sync_controller(c)

        for sensor in db.get_sensors(controller):
            s = Sensor(name=sensor.name, id=sensor.id, controller=c)
            store.prepare_for_sync_sensor(s)
            store.sync(
                sensor=s,
                db=db,
                serializer=getattr(serializers, args.serializer)(),
                stream_wrapper=AesStreamWrapper(key=db.get_encryption_key()),
            )

    logger.info("done")


if __name__ == "__main__":
    main()
