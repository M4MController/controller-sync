import base64
import csv
import io
import json
import typing

from database import SensorData


class BaseSerializer:
    def serialize(self, out_stream: io.IOBase, data: typing.List[SensorData]):
        return self._serialize_unencrypted(out_stream, data)

    def _serialize_unencrypted(self, out_stream: io.IOBase, data: typing.List[SensorData]):
        pass


class CsvVerboseSerializer(BaseSerializer):
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
            csv_writer = csv.writer(out_stream, delimiter=',', quotechar='"')
            for row in data:
                csv_writer.writerow([row.data["timestamp"], row.data["value"].value])


class CsvRawSerializer(BaseSerializer):
    def _serialize_unencrypted(self, out_stream: io.IOBase, data: typing.List[SensorData]):
        if not data:
            return

        csv_writer = csv.writer(out_stream, delimiter=',', quotechar='"')
        csv_writer.writerow(["value", "signer", "sign"])

        for row in data:
            csv_writer.writerow([
                json.dumps(row.data),
                str(base64.b64encode(row.signer), encoding='utf-8') if row.signer else "",
                str(base64.b64encode(row.sign), encoding='utf-8') if row.sign else "",
            ])
