import socket
import struct
import json

"condition(1)|sql_length(8)|p_length(8)|sql|p"


class Connection:
    def __init__(self, host, port):
        self.socket = socket.socket()
        self.socket.connect((host, port))

    def execute(self, sql, parameters=None):
        condition = b"1"
        sql = sql.encode()
        sql = struct.pack("!{}s".format(len(sql)), sql)
        sql_length = struct.pack("!Q", len(sql))
        if parameters is not None:
            parameters_data = json.dumps(parameters)
        else:
            parameters_data = b""

        p_length = struct.pack("!Q", len(parameters_data))
        data = condition + sql_length + p_length + sql + parameters_data
        self.socket.sendall(data)
        self.recv_result()

    def commit(self):
        condition = b"2"
        length = struct.pack("!QQ", 0, 0)
        self.socket.sendall(condition + length)

    def recv(self):
        pass

    def recv_result(self):
        condition = self.socket.recv(1)
        length, = struct.unpack("!Q", self.socket.recv(8))
        if length:
            data = self.socket.recv(length)
            result = json.loads(data.decode())
        else:
            result = None
        print(result)
        return condition, result

def recv_all(sock, length):
    data = b""
    while len:
        _data = sock.recv(len)
        length = length - len(_data)
        data += _data
    return data


conn = Connection("localhost", 7887)

conn.execute("SELECT * FROM fuck")
conn.commit()
