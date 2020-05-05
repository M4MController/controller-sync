import hashlib
import os
import typing

from Crypto import Random
from Crypto.Cipher import AES

from core.utils import StreamWrapper


class AesStreamWrapper(StreamWrapper):
    def __init__(self, key: typing.Union[str, bytes], *args, **kwargs):
        super().__init__(*args, **kwargs)

        if isinstance(key, str):
            key = key.encode('utf-8')

        self.__block_size = AES.block_size
        self.__key = hashlib.sha256(key).digest()
        self.__cipher = None
        self.__write_buffer = bytearray()
        self.__read_buffer = False

    def __call__(self, *args, **kwargs):
        result = super().__call__(*args, **kwargs)
        self.__cipher = None
        self.__write_buffer = None
        self.__read_buffer = False
        return result

    def read(self, size: int = -1) -> typing.Optional[bytes]:
        if self.__cipher is None:
            iv = self._stream.read(self.__block_size)
            self.__cipher = AES.new(self.__key, AES.MODE_CBC, iv)

        if self.__read_buffer == False:
            self.__read_buffer = self.__unpad(self.__cipher.decrypt(self._stream.read()))
        if size == -1:
            result = self.__read_buffer
            self.__read_buffer = None
            return result
        else:
            result = self.__read_buffer[:size]
            self.__read_buffer = self.__read_buffer[size:]
            return result

    def write(self, b: typing.Union[bytes, bytearray]) -> typing.Optional[int]:
        if self.__cipher is None:
            iv = Random.new().read(self.__block_size)
            self.__cipher = AES.new(self.__key, AES.MODE_CBC, iv)
            self._stream.write(iv)

        if self.__write_buffer is None:
            self.__write_buffer = bytearray()

        # full the buffer and encrypt it if possible
        move_to_buffer_count = self.__block_size - len(self.__write_buffer)
        self.__write_buffer += b[:move_to_buffer_count]
        if len(self.__write_buffer) % self.__block_size == 0:
            self._stream.write(self.__cipher.encrypt(self.__write_buffer))
            self.__write_buffer = bytearray()
        b = b[move_to_buffer_count:]

        # encrypt remaining part (with no tail)
        to_write_count = (len(b) // self.__block_size) * self.__block_size
        self._stream.write(self.__cipher.encrypt(b[:to_write_count]))
        b = b[to_write_count:]

        # buffer the tail
        self.__write_buffer += b

    def close(self) -> None:
        if self.writable() and self.__write_buffer is not None:
            self.seek(0, os.SEEK_END)

            to_pad = self.__block_size - len(self.__write_buffer) % self.__block_size
            self._stream.write(self.__cipher.encrypt(self.__write_buffer + bytearray([to_pad] * to_pad)))
            self.__write_buffer = bytearray()
        super().close()

    @staticmethod
    def __unpad(s):
        return s[:-ord(s[len(s) - 1:])]
