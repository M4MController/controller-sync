import calendar
import datetime
import dateutil.relativedelta
import io
import typing


class DateTimeRange:
    def __init__(self, start: datetime.datetime, end: datetime.datetime):
        self.start = start
        self.end = end

    @staticmethod
    def __move_from_now(**kwargs):
        return datetime.datetime.now() + dateutil.relativedelta.relativedelta(**kwargs)

    @staticmethod
    def month(point: typing.Union[datetime.datetime, int, None] = None):
        if type(point) == int:
            return DateTimeRange.day(DateTimeRange.__move_from_now(months=point))

        if point is None:
            point = datetime.datetime.now()

        return DateTimeRange(
            start=datetime.datetime(year=point.year, month=point.month, day=1),
            end=datetime.datetime(
                year=point.year,
                month=point.month,
                day=calendar.monthrange(point.year, point.month)[1],
            ),
        )

    @staticmethod
    def day(point: typing.Union[datetime.datetime, int, None] = None):
        if type(point) == int:
            return DateTimeRange.day(DateTimeRange.__move_from_now(days=point))

        if point is None:
            point = datetime.datetime.now()

        return DateTimeRange(
            start=datetime.datetime(year=point.year, month=point.month, day=point.day),
            end=datetime.datetime(
                year=point.year,
                month=point.month,
                day=point.day,
                hour=23,
                minute=59,
                second=59,
                microsecond=999999,
            ),
        )

    def __str__(self):
        return "{} - {}".format(self.start, self.end)


class StreamWrapper(io.RawIOBase):
    def __init__(self, stream: io.BufferedIOBase = None, close_source: bool = False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if stream:
            self(stream)
        self.__close_source = close_source

    def __call__(self, stream: io.BufferedIOBase):
        self._stream = stream
        return self

    def __getattr__(self, item):
        return getattr(self._stream, item)

    def read(self, *args, **kwargs) -> typing.Optional[bytes]:
        return self._stream.read( *args, **kwargs)

    def seek(self, *args, **kwargs) -> int:
        return self._stream.seek(*args, **kwargs)

    def write(self, *args, **kwargs) -> typing.Optional[int]:
        return self._stream.write(*args, **kwargs)

    def writable(self) -> bool:
        return self._stream.writable()

    def close(self):
        if self.__close_source:
            self._stream.close()
