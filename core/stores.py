import datetime
import io
import logging
import os
import pathlib
import typing

from collections import namedtuple

import easywebdav
import requests

from core.database import DatabaseManager
from core.serializers import BaseSerializer
from core.utils import DateTimeRange, StreamWrapper, find_in_list

logger = logging.getLogger(__name__)


class File:
    def __init__(self, name: str, is_dir: bool):
        self.name = name
        self.is_dir = is_dir

    def __str__(self):
        return self.name

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        else:
            return self.name == other.name and self.is_dir == other.is_dir


Controller = str


class Sensor:
    def __init__(self, id: str = None, name: str = None, controller: str = None):
        self.name = name
        self.id = id
        self.controller = controller

    def __str__(self):
        return "{id}".format(name=self.name, id=self.id)

    def __eq__(self, other):
        return self.id == other.id

    @staticmethod
    def from_name(name: str):
        parts = name.split('.')
        if len(parts) == 2:
            [sensor_id, sensor_name] = parts
            return Sensor(id=sensor_id, name=sensor_name)


class BaseStore:
    ROOT = "M4M"
    __SENSOR_NAME_PREFIX = "."

    def __init__(self):
        self.__create_root_dir()

    def get_controllers(self):
        return [file.name for file in self._ls(self.__join()) if file.is_dir]

    def get_sensors(self, controller: Controller) -> typing.List[Sensor]:
        result = []
        for file in self._ls(self.__join(str(controller))):
            if file.is_dir:
                sensor_id = file.name
                files = self._ls(self.__join(str(controller), sensor_id))
                sensor_file = find_in_list(files, lambda file: file.name.startswith(self.__SENSOR_NAME_PREFIX))
                if sensor_file:
                    sensor_name = sensor_file.name[1:]
                else:
                    sensor_name = "No Name"
                result.append(Sensor(id=sensor_id, name=sensor_name, controller=controller))
        return result

    def _upload(self, stream: io.IOBase, path: str):
        raise NotImplementedError

    def _get_download_stream(self, path: str):
        raise NotImplementedError

    def _create_folder(self, path: str):
        raise NotImplementedError

    def _ls(self, path: str) -> typing.List[File]:
        raise NotImplementedError

    def __join(self, *paths: str) -> str:
        return os.path.join(self.ROOT, *paths)

    def __contains_dir(self, path: str, dir_name: str) -> bool:
        return find_in_list(self._ls(self.__join(path)), lambda file: file.is_dir and file.name == dir_name) is not None

    def __contains_file(self, path: str, file_name: str) -> bool:
        return find_in_list(
            self._ls(self.__join(path)),
            lambda file: not file.is_dir and file.name == file_name,
        ) is not None

    @staticmethod
    def __get_file_name_for_day(date: datetime.datetime) -> str:
        return "{year}.{month}.{day}.m4m".format(
            year=date.year,
            month=date.month,
            day=date.day,
        )

    def __create_root_dir(self):
        files = self._ls("/")
        if self.ROOT not in [file.name for file in files]:
            logger.info("creating root folder")
            self._create_folder(self.ROOT)

    def prepare_for_sync_controller(self, controller: Controller):
        remote_controllers = self.get_controllers()
        if controller not in remote_controllers:
            self._create_folder(self.__join(str(controller)))

    def prepare_for_sync_sensor(self, sensor: Sensor):
        remote_sensors = self.get_sensors(sensor.controller)

        if sensor not in remote_sensors:
            self._create_folder(self.__join(str(sensor.controller), str(sensor)))

    def sync(self, sensor: Sensor, db: DatabaseManager, serializer: BaseSerializer, stream_wrapper: StreamWrapper):
        files = self._ls(self.__join(str(sensor.controller), str(sensor)))
        file_sensor_name = find_in_list(files, lambda file: file.name.startswith(self.__SENSOR_NAME_PREFIX))

        sensor_name_file_path = self.__join(str(sensor.controller), str(sensor),
                                            self.__SENSOR_NAME_PREFIX + sensor.name)

        if not file_sensor_name:
            self._upload(io.BytesIO(), sensor_name_file_path)
        elif file_sensor_name.name[1:] != sensor.name:
            self._rm(self.__join(str(sensor.controller), str(sensor), file_sensor_name.name))
            self._upload(io.BytesIO(), sensor_name_file_path)

        first_date = db.get_first_sensor_data_date(sensor.id)
        first_date_range = DateTimeRange.day(first_date)
        i = 0

        while True:
            current_range = DateTimeRange.day(i)
            if current_range.start < first_date_range.start:
                break

            file_name = self.__get_file_name_for_day(current_range.start)
            if file_name in files:
                i -= 1
                continue

            sensor_data_file_name = self.__join(str(sensor.controller), str(sensor), file_name)

            data = db.get_sensor_data(sensor_id=sensor.id, datetime_range=current_range)
            if not data:
                i -= 1
                continue

            logger.info("Converting %s", sensor_data_file_name)
            with io.BytesIO() as temp_stream:
                with stream_wrapper(stream=temp_stream) as wrapped_stream:
                    serializer.serialize(
                        out_stream=wrapped_stream,
                        data=data,
                    )

                logger.info("Saving %s", sensor_data_file_name)
                temp_stream.seek(0)
                self._upload(temp_stream, sensor_data_file_name)

                i -= 1

    def get(self, sensor: Sensor, range: DateTimeRange, stream_wrapper: StreamWrapper) -> typing.List[bytes]:
        result = []

        files = self._ls(self.__join(sensor.controller, str(sensor)))
        current_day = range.start
        while current_day <= range.end:
            file_name = self.__get_file_name_for_day(current_day)
            if find_in_list(files, lambda file: file.name == file_name):
                with self._get_download_stream(self.__join(str(sensor.controller), str(sensor), file_name)) as file:
                    with stream_wrapper(file) as stream:
                        result.append(stream.read())
            current_day = current_day + datetime.timedelta(days=1)

        return result


class LocalStore(BaseStore):
    def __init__(self, root: str, *args, **kwargs):
        self.__root = root
        super().__init__(*args, **kwargs)

    def __normalize_path(self, path: str) -> str:
        if path.startswith("/"):
            path = path[1:]
        return os.path.join(self.__root, path)

    def _create_folder(self, path: str):
        os.makedirs(self.__normalize_path(path))

    def _ls(self, path: str) -> typing.List[File]:
        path = self.__normalize_path(path)
        return [File(name=f, is_dir=os.path.isdir(os.path.join(path, f))) for f in os.listdir(path)]

    def _rm(self, path: str):
        os.remove(self.__normalize_path(path))

    def _upload(self, stream: io.IOBase, path: str):
        path = self.__normalize_path(path)

        pathlib.Path(path).write_bytes(stream.getbuffer())

    def _get_download_stream(self, path: str):
        return open(self.__normalize_path(path), 'rb')


class WebDavStore(BaseStore):
    def __init__(self, uri: str, auth=None, username: str = None, password: str = None, protocol=None, *args, **kwargs):
        self.__webdav = easywebdav.Client(
            uri,
            auth=auth,
            username=username,
            password=password,
            protocol=protocol,
        )
        super().__init__(*args, **kwargs)

    def _ls(self, path: str) -> typing.List[File]:
        return [
            File(
                name=os.path.basename(file.name.strip('/')),
                is_dir=file.name.endswith('/'),
            ) for file in self.__webdav.ls(path) if '/' + path + '/' != file.name
        ]

    def _create_folder(self, path: str):
        self.__webdav.mkdir(path)

    def _rm(self, path: str):
        self.__webdav.delete(path)

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
