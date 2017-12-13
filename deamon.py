import sqlite3
import queue
import socket
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
import pickle

"condition(1byte)|length(8byte)|result"


class RemoteConnectException(Exception):
    pass


class DB:
    def __init__(self, path):
        self.path = path
        self.connection = sqlite3.connect(path)
        self.task_queue = queue.Queue()
        self.bind_sock = None
        self.bind_thread = Thread(target=self.sock_handler)


    def run(self):
        self.init_bind_sock()
        print("server running!")
        self.bind_thread.start()
        self.work()

    def init_bind_sock(self, host="localhost", port=7887, listen=512):
        self.bind_sock = socket.socket()
        self.bind_sock.bind((host, port))
        self.bind_sock.listen(512)

        self.thread_pool = ThreadPoolExecutor(512)

    def sock_handler(self):
        while True:
            sock, addr = self.bind_sock.accept()
            self.thread_pool.submit(self.sock_parser, sock)

    def sock_parser(self, sock):
        while True:
            try:
                condition = recv_all(sock, 1).decode()
                length = int(recv_all(sock, 8).decode())
                exe = recv_all(sock, length).decode()
                self.task_queue.put((condition, length, exe, sock))
            except RemoteConnectException:
                break

    @staticmethod
    def send_result(sock, result):
        condition, resp = result
        condition = condition.encode()
        resp = resp.encode()
        length = len(resp)

        sock.sendall(condition + length + resp)

    def work(self):
        while True:
            condition, _, exe, sock = self.task_queue.get()

            if condition == "0":  # close
                res = self.close()
            elif condition == "1": # execute
                res = self.execute()
            elif condition == "2":  # commit
                res = self.connection.commit()

            self.thread_pool.submit(DB.send_result, sock, res)

    def close(self):
        self.bind_sock.close()
        while len(self.task_queue) != 0:
            pass
        self.connection.close()
        return b"1", b"connection closed"

    def execute(self, sql):
        result = self.connection.execute(sql)
        return b"1", pickle.dumps(result)

    def commit(self):
        self.connection.commit()
        return b"1", "commit successfully"




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