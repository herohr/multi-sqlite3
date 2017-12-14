import sqlite3
import queue
import socket
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
import json
import struct
import logging
import time

"condition(1byte)|length(8byte)|result"
struct_str = "1sQ"


class RemoteConnectException(Exception):
    pass


class DB:
    def __init__(self, path):
        self.path = path
        self.connection = sqlite3.connect(path)
        self.task_queue = queue.Queue()
        self.bind_sock = None
        self.thread_pool = None
        self.bind_thread = Thread(target=self.sock_handler)
        self.connections_dict = {}

    def run(self):
        self.init_bind_sock()
        print("server running!")
        self.bind_thread.start()
        self.work()

    def init_bind_sock(self, host="localhost", port=7887, listen=512):
        self.bind_sock = socket.socket()
        self.bind_sock.bind((host, port))
        self.bind_sock.listen(listen)

        self.thread_pool = ThreadPoolExecutor(512)

    def sock_handler(self):
        while True:
            sock, addr = self.bind_sock.accept()
            self.connections_dict[sock] = 0
            self.thread_pool.submit(self.sock_parser, sock)

    def sock_parser(self, sock):
        while True:
            try:
                condition, = struct.unpack("!1s", recv_all(sock, 1))
                sql_length, p_length = struct.unpack("!QQ", recv_all(sock, 16))
                execute_sql, = struct.unpack("!{}s".format(str(sql_length)), recv_all(sock, sql_length))
                execute_sql = execute_sql.decode()
                if p_length:
                    parameters_data = recv_all(sock, p_length)
                    parameters = json.loads(parameters_data)
                else:
                    parameters = ()

                self.connections_dict[sock] += 1
                self.task_queue.put((condition, execute_sql, parameters, sock))

            except RemoteConnectException:
                break
            except ConnectionResetError:
                break
            except BaseException as err:
                logging.exception(err)
                raise err

    @staticmethod
    def send_result(sock, result):
        try:
            condition, resp = result
            length = struct.pack("!Q", len(resp))
            sock.sendall(condition + length + resp)
        except Exception as e:
            logging.exception(e)
            raise e

    def work(self):
        while True:
            condition, sql, parameters, sock = self.task_queue.get()
            if condition == b"0":  # close
                res = self.close(sock)
            elif condition == b"1":  # execute
                res = self.execute(sql, parameters or None)
            else:  # commit
                res = self.commit()
            self.thread_pool.submit(DB.send_result, sock, res)

    def close(self, sock):
        def defer_close():
            while self.connections_dict[sock]:
                time.sleep(1)
            del self.connections_dict[sock]
            sock.close()
        self.thread_pool.submit(defer_close)
        return b"1", b"connection closed"

    def execute(self, sql, parameters=None):
        print("Exe " + sql)
        if parameters is not None:
            result = self.connection.execute(sql, parameters).fetchall()
        else:
            result = self.connection.execute(sql).fetchall()
        print("Result", result)
        return b"1", json.dumps(result).encode()

    def commit(self):
        self.connection.commit()
        print("COMMITE")
        return b"1", b"commit successfully"


def recv_all(sock, length):
    data = b""
    while length:
        _data = sock.recv(length)
        if _data == b"":
            raise RemoteConnectException("Connection closed!")
        length = length - len(_data)
        data += _data
    return data


a = DB("test.db")
a.run()
