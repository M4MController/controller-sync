import io
import logging
import os
import shutil
import typing

from database import DatabaseManager
from serializers import BaseSerializer
from utils import DateTimeRange


logger = logging.getLogger(__name__)


class BaseSynchronizer:
    ROOT = "M4M"

    def __init__(self, database: DatabaseManager, serializer: BaseSerializer):
        self.__database = database
        self.__serializer = serializer

    def _upload(self, stream: io.IOBase, path: str):
        pass

    def _create_folder(self, path: str):
        pass

    def _ls(self, path: str) -> typing.List[str]:
        pass

    def __get_file_name_for_day(self, sensor_id: int, range: DateTimeRange) -> str:
        return os.path.join(self.ROOT, "{sensor_id}.{year}.{month}.{day}.m4m".format(
            sensor_id=sensor_id,
            year=range.start.year,
            month=range.start.month,
            day=range.start.day,
        ))

    def sync(self):
        files = self._ls("/")
        if self.ROOT not in files:
            self._create_folder(self.ROOT)

        files = self._ls(self.ROOT)

        for sensor in self.__database.get_sensors():
            first_date = self.__database.get_first_sensor_data_date(sensor.id)
            i = -1

            while True:
                current_range = DateTimeRange.day(i)
                if current_range.start < first_date:
                    break

                file_name = self.__get_file_name_for_day(sensor.id, current_range)
                if file_name in files:
                    break

                logger.info("Converting %s", file_name)
                temp_stream = io.StringIO()
                self.__serializer.serialize(
                    out_stream=temp_stream,
                    data=self.__database.get_sensor_data(sensor_id=sensor.id, datetime_range=current_range))
                temp_stream.seek(0)

                logger.info("Saving %s", file_name)
                self._upload(temp_stream, file_name)
                temp_stream.close()

                i -= 1


class LocalSynchronizer(BaseSynchronizer):
    def __init__(self, root: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__root = root

    def __normalize_path(self, path: str) -> str:
        if path.startswith("/"):
            path = path[1:]
        return os.path.join(self.__root, path)

    def _create_folder(self, path: str):
        os.makedirs(self.__normalize_path(path))

    def _ls(self, path: str):
        path = self.__normalize_path(path)
        return [f for f in os.listdir(path)]

    def _upload(self, stream: io.IOBase, path: str):
        path = self.__normalize_path(path)
        stream.seek(0)
        with open(path, "w+") as f:
            shutil.copyfileobj(stream, f)
