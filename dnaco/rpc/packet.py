class RpcPacket:
    def __init__(self, trace_id, pkg_id):
        self.trace_id = trace_id
        self.pkg_id = pkg_id
        self.rev = None
        self.write_to_wal = None


class RpcRequest(RpcPacket):
    # - Operation Type: 2bit (READ, WRITE, RW, COMPUTE)
    OP_TYPE_READ = 0
    OP_TYPE_WRITE = 1
    OP_TYPE_RW = 2
    OP_TYPE_COMPUTE = 3

    # - Send Result to: 2bit (CALLER, STORE_IN_MEMORY, STORE_WITH_ID, FORWARD_TO)
    SEND_RESULT_TO_CALLER = 0
    STORE_RESULT_IN_MEMORY = 1
    STORE_RESULT_WITH_ID = 2
    FORWARD_RESULT_TO = 3

    def __init__(self, trace_id, pkg_id, op_type, send_result_to, result_id, request_id, body):
        super(RpcRequest, self).__init__(trace_id, pkg_id)
        self.op_type = op_type
        self.send_result_to = send_result_to
        self.result_id = result_id
        self.request_id = request_id
        self.body = body

    def __repr__(self) -> str:
        return 'RpcRequest [traceId=' + repr(self.trace_id) \
               + ', pkg_id=' + repr(self.pkg_id) \
               + ', op_type=' + repr(self.op_type) \
               + ', request_id=' + repr(self.request_id) \
               + ', body=' + repr(self.body) \
               + ', send_result_to=' + repr(self.send_result_to) \
               + ', result_id=' + repr(self.result_id) \
               + ']'


class RpcResponse(RpcPacket):
    # - Operation Status: 2bit (SUCCEEDED, FAILED, CANCELLED, _)
    OP_STATUS_SUCCEEDED = 0
    OP_STATUS_FAILED = 1
    OP_STATUS_CANCELLED = 2

    def __init__(self, trace_id, pkg_id, op_status, queue_time, exec_time, body):
        super(RpcResponse, self).__init__(trace_id, pkg_id)
        self.op_status = op_status
        self.queue_time = queue_time
        self.exec_time = exec_time
        self.body = body

    def __repr__(self) -> str:
        return 'RpcResponse [traceId=' + repr(self.trace_id) \
               + ', pkg_id=' + repr(self.pkg_id) \
               + ', op_status=' + repr(self.op_status) \
               + ', queue_time=' + repr(self.queue_time) \
               + ', exec_time=' + repr(self.exec_time) \
               + ', body=' + repr(self.body) \
               + ']'


def parse_rpc_packet(frame):
    # RPC packets are composed of:
    # - Packet Type: 2bit (REQUEST, RESPONSE, EVENT, CONTROL)
    # - Trace Id length: 3bit (1-8 bytes ID)
    # - Packet Id length: 3bit (1-8 bytes ID)
    #  +----+-----+-----+ +----------+ +-----------------------+
    #  | 11 | 111 | 111 | | Trace Id | | Packet Id (1-8 bytes) |
    #  +----+-----+-----+ +----------+ +-----------------------+
    #  0    2     5     8
    rpc_head = frame.read_byte()
    pkg_type = (rpc_head >> 6) & 0x3
    trace_id_len = 1 + ((rpc_head >> 3) & 0x7)
    pkg_id_len = 1 + (rpc_head & 0x7)

    trace_id = int.from_bytes(frame.read(trace_id_len), byteorder='little')
    pkg_id = int.from_bytes(frame.read(pkg_id_len), byteorder='little')

    if pkg_type == 0:
        # RPC Request packets are composed of:
        # - Operation Type: 2bit (READ, WRITE, RW, COMPUTE)
        # - Send Result to: 2bit (CALLER, STORE_IN_MEMORY, STORE_WITH_ID, FORWARD_TO)
        #  +----+----+--------+--------+ +-----------+ +------------+
        #  | 11 | 11 | ------ | ------ | | Result Id | | Request Id |
        #  +----+----+--------+--------+ +-----------+ +------------+
        #  0    2    4        8       16
        req_head = int.from_bytes(frame.read(2), byteorder='big')
        op_type = (req_head >> 14) & 0x3
        send_result_to = (req_head >> 12) & 0x3
        result_id_len = (req_head >> 6) & 0x3f
        request_id_len = 1 + (req_head & 0x3f)
        result_id = frame.read(result_id_len)
        request_id = frame.read(request_id_len)
        data = frame.read_all()
        return RpcRequest(trace_id, pkg_id, op_type, send_result_to, result_id, request_id, data)

    if pkg_type == 1:
        # RPC Response Packets are composed of:
        # - Operation Status: 2bit (SUCCEEDED, FAILED, CANCELLED, _)
        # - Queue Time length: 3bit
        # - Exec Time length: 3bit
        #   +----+-----+-----+ +---------------+ +--------------+
        #   | 11 | 111 | 111 | | Queue Time ns | | Exec Time ns |
        #   +----+-----+-----+ +---------------+ +--------------+
        #   0    2     5     8
        resp_head = frame.read_byte()
        op_status = (resp_head >> 6) & 0x3
        queue_time_len = 1 + ((resp_head >> 3) & 0x7)
        exec_time_len = 1 + (resp_head & 0x7)
        queue_time = int.from_bytes(frame.read(queue_time_len), byteorder='little')
        exec_time = int.from_bytes(frame.read(exec_time_len), byteorder='little')
        data = frame.read_all()
        return RpcResponse(trace_id, pkg_id, op_status, queue_time, exec_time, data)

    raise NotImplementedError


def build_rpc_request_head(op_type, request_id, send_result_to=0, result_id=None):
    # RPC Request packets are composed of:
    # - Operation Type: 2bit (READ, WRITE, RW, COMPUTE)
    # - Send Result to: 2bit (CALLER, STORE_IN_MEMORY, STORE_WITH_ID, FORWARD_TO)
    #  +----+----+--------+--------+ +-----------+ +------------+
    #  | 11 | 11 | ------ | ------ | | Result Id | | Request Id |
    #  +----+----+--------+--------+ +-----------+ +------------+
    #  0    2    4        8       16
    req_head = (op_type << 14) | (send_result_to << 12)
    req_head |= (len(result_id) << 6) if result_id else 0
    req_head |= (len(request_id) - 1)
    return req_head.to_bytes(2, byteorder='big') + (result_id if result_id else b'') + request_id


def build_rpc_response_head(op_status, queue_time, exec_time):
    # RPC Response Packets are composed of:
    # - Operation Status: 2bit (SUCCEEDED, FAILED, CANCELLED, _)
    # - Queue Time length: 3bit
    # - Exec Time length: 3bit
    #   +----+-----+-----+ +---------------+ +--------------+
    #   | 11 | 111 | 111 | | Queue Time ns | | Exec Time ns |
    #   +----+-----+-----+ +---------------+ +--------------+
    #   0    2     5     8
    queue_time_len = ((queue_time.bit_length() + 7) // 8) if queue_time > 0 else 1
    exec_time_len = ((exec_time.bit_length() + 7) // 8) if exec_time > 0 else 1
    resp_head = (op_status << 6) | ((queue_time_len - 1) << 3) | (exec_time_len - 1)
    return bytes([resp_head]) + queue_time.to_bytes(queue_time_len, byteorder='little') + exec_time.to_bytes(
        exec_time_len, byteorder='little')


def build_rpc_head(pkg_type, trace_id, pkg_id):
    # RPC packets are composed of:
    # - Packet Type: 2bit (REQUEST, RESPONSE, EVENT, CONTROL)
    # - Trace Id length: 3bit (1-8 bytes ID)
    # - Packet Id length: 3bit (1-8 bytes ID)
    #  +----+-----+-----+ +----------+ +-----------------------+
    #  | 11 | 111 | 111 | | Trace Id | | Packet Id (1-8 bytes) |
    #  +----+-----+-----+ +----------+ +-----------------------+
    #  0    2     5     8
    trace_id_len = ((trace_id.bit_length() + 7) // 8)
    pkg_id_len = ((pkg_id.bit_length() + 7) // 8)
    rpc_head = (pkg_type << 6) | ((trace_id_len - 1) << 3) | (pkg_id_len - 1)
    return bytes([rpc_head]) + trace_id.to_bytes(trace_id_len, byteorder='little') + pkg_id.to_bytes(pkg_id_len, byteorder='little')


class RpcPacketBuilder:
    def __init__(self, rev):
        self.trace_id = 0
        self.packet_id = 0
        self.rev = rev

    def next_trace_id(self):
        self.trace_id += 1
        return self.trace_id

    def next_packet_id(self):
        self.packet_id += 1
        return self.packet_id

    def new_request(self, trace_id, op_type, request_id, body, send_result_to=0, result_id=None):
        req = build_rpc_request_head(op_type, request_id, send_result_to, result_id) + body
        req = build_rpc_head(0, trace_id, self.next_packet_id()) + req
        return req

    def new_response(self, trace_id, pkg_id, op_status, queue_time, exec_time, body):
        resp = build_rpc_response_head(op_status, queue_time, exec_time) + body
        resp = build_rpc_head(1, trace_id, pkg_id) + resp
        return resp
