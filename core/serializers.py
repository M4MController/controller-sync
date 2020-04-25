import base64
import csv
import io
import json
import typing

from core.database import SensorData


class BaseSerializer:
    def serialize(self, out_stream: io.IOBase, data: typing.List[SensorData]):
        return self._serialize(out_stream, data)

    def deserialize(self, input_stream: io.IOBase):
        return input_stream.read()

    def _serialize(self, out_stream: io.IOBase, data: typing.List[SensorData]):
        pass


class CsvVerboseSerializer(BaseSerializer):
    def _serialize(self, out_stream: io.IOBase, data: typing.List[SensorData]):
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
            csv_writer = csv.writer(out_stream, delimiter=',', quotechar='"')
            for row in data:
                csv_writer.writerow([row.data["timestamp"], row.data["value"].value])


class CsvRawSerializer(BaseSerializer):
    def _serialize(self, out_stream: io.IOBase, data: typing.List[SensorData]):
        if not data:
            return

        encoding = "utf-8"
        delimeter = '\t'

        out_stream.write("value{d}signer{d}sign\n".format(d=delimeter).encode(encoding))

        for row in data:
            out_stream.write("{value}{d}{signer}{d}{sign}\n".format(
                d=delimeter,
                value=json.dumps(row.data),
                signer=str(base64.b64encode(row.signer), encoding='utf-8') if row.signer else "",
                sign=str(base64.b64encode(row.sign), encoding='utf-8') if row.sign else ""
            ).encode(encoding))
