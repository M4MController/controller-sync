import calendar
import datetime
import dateutil.relativedelta
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
