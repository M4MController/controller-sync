import csv
import io
import typing

from database import SensorData


class Serializer:
    def serialize(self, out_stream: io.IOBase, data: typing.List[SensorData]):
        return self._serialize_unencrypted(out_stream, data)

    def _serialize_unencrypted(self, out_stream: io.IOBase, data: typing.List[SensorData]):
        pass


class CsvSerializer(Serializer):
    def _serialize_unencrypted(self, out_stream: io.IOBase, data: typing.List[SensorData]):
        if not data:
            return

        first_value = data[0].data["value"]
        is_multi_value = type(first_value) == dict

        if is_multi_value:
            csv_writer = csv.DictWriter(out_stream, {"timestamp": 1, **first_value}, delimiter=',', quotechar='"')
            csv_writer.writeheader()

            for record in data:
                csv_writer.writerow({"timestamp": record.data["timestamp"], **record.data["value"]})
        else:
            for row in data:
                csv_writer = csv.writer(out_stream, delimiter=',', quotechar='"')
                csv_writer.writerow([row.data["timestamp"], row.data["value"].value])
