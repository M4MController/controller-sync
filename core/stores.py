import datetime
import io
import logging
import os
import pathlib
import typing

import easywebdav
import requests

from core.database import DatabaseManager
from core.serializers import BaseSerializer
from core.utils import DateTimeRange, StreamWrapper

logger = logging.getLogger(__name__)


class BaseStore:
    ROOT = "M4M"

    def __init__(self, serializer: BaseSerializer, database: DatabaseManager = None, stream_wrapper: type = None):
        self.__database = database
        self.__serializer = serializer
        self.__stream_wrapper = stream_wrapper or StreamWrapper()

    def _upload(self, stream: io.IOBase, path: str):
        pass

    def _get_download_stream(self, path: str):
        pass

    def _create_folder(self, path: str):
        pass

    def _ls(self, path: str) -> typing.List[str]:
        pass

    @staticmethod
    def __get_file_name_for_day(sensor_id: int, date: datetime.datetime) -> str:
        return "{sensor_id}.{year}.{month}.{day}.m4m".format(
            sensor_id=sensor_id,
            year=date.year,
            month=date.month,
            day=date.day,
        )

    def __get_m4m_files(self):
        files = self._ls("/")
        if self.ROOT not in files:
            logger.info("creating root folder")
            self._create_folder(self.ROOT)
        return self._ls(self.ROOT)

    def sync(self):
        files = self.__get_m4m_files()

        for sensor in self.__database.get_sensors():
            first_date = self.__database.get_first_sensor_data_date(sensor.id)
            first_date_range = DateTimeRange.day(first_date)
            i = 0

            while True:
                current_range = DateTimeRange.day(i)
                if current_range.start < first_date_range.start:
                    break

                file_name = self.__get_file_name_for_day(sensor.id, current_range.start)
                if file_name in files:
                    i -= 1
                    continue

                data = self.__database.get_sensor_data(sensor_id=sensor.id, datetime_range=current_range)
                if not data:
                    i -= 1
                    continue

                logger.info("Converting %s", file_name)
                with io.BytesIO() as temp_stream:
                    with self.__stream_wrapper(stream=temp_stream) as wrapped_stream:
                        self.__serializer.serialize(
                            out_stream=wrapped_stream,
                            data=data,
                        )

                    logger.info("Saving %s", file_name)
                    temp_stream.seek(0)
                    self._upload(temp_stream, os.path.join(self.ROOT, file_name))

                i -= 1

    def get(self, sensor_id: int, range: DateTimeRange) -> typing.List[bytes]:
        result = []
        files = self.__get_m4m_files()
        current_day = range.start
        while current_day <= range.end:
            file_name = self.__get_file_name_for_day(sensor_id, current_day)
            if file_name in files:
                with self._get_download_stream(os.path.join(self.ROOT, file_name)) as file:
                    with self.__stream_wrapper(file) as stream:
                        result.append(stream.read())
            current_day = current_day + datetime.timedelta(days=1)

        return result


class LocalStore(BaseStore):
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

        pathlib.Path(path).write_bytes(stream.getbuffer())

    def _get_download_stream(self, path: str):
        return open(self.__normalize_path(path), 'rb')


class WebDavStore(BaseStore):
    def __init__(self, uri: str, auth=None, username: str = None, password: str = None, protocol=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__webdav = easywebdav.Client(
            uri,
            auth=auth,
            username=username,
            password=password,
            protocol=protocol,
        )

    def _ls(self, path: str) -> typing.List[str]:
        return [os.path.basename(file.name.strip('/')) for file in self.__webdav.ls(path)]

    def _create_folder(self, path: str):
        self.__webdav.mkdir(path)

    def _upload(self, stream: io.IOBase, path: str):
        self.__webdav._upload(stream, path)

    def _get_download_stream(self, path: str):
        response = self.__webdav._send('GET', path, 200, stream=True)
        result = io.BytesIO()
        self.__webdav._download(result, response)
        result.seek(0)
        return result


class YaDiskSynchronizer(WebDavStore):
    class HTTPBearerAuth(requests.auth.AuthBase):
        def __init__(self, token):
            self.token = token

        def __eq__(self, other):
            return self.token == getattr(other, 'token', None)

        def __ne__(self, other):
            return not self == other

        def __call__(self, r):
            r.headers['Authorization'] = 'Bearer ' + self.token
            return r

    def __init__(self, token: str, *args, **kwargs):
        super().__init__(
            uri="webdav.yandex.ru",
            protocol="https",
            auth=YaDiskSynchronizer.HTTPBearerAuth(token),
            *args,
            **kwargs,
        )
