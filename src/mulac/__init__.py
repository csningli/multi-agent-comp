
import os, time, itertools, datetime
import queue, multiprocessing, threading, zmq

def get_current_time() :
    return datetime.datetime.now().strftime("%Y/%m/%d_%H:%M:%S.%f")

def get_datetime_stamp() :
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def get_one_item_in_queue(q, max_num_items = 10) :
    item = None
    try:
        item = q.get_nowait()
    except queue.Empty as e:
        pass
    return item

def put_one_item_to_queue(q, item) :
    try :
        q.put_nowait(item)
    except queue.Full as e:
        pass

def get_all_items_in_queue(q, max_num_items = 10) :
    items = []
    for i in range(0, max_num_items):
        try:
            items.append(q.get_nowait())
        except queue.Empty as e:
            break
    return items

def put_items_to_queue(q, items) :
    for item in items :
        try:
            q.put_nowait(item)
        except queue.Full as e:
            break

class Message(object) :
    def __init__(self, src = "", dest = "", topic = "", content = "") :
        self.src = src
        self.dest = dest
        self.topic = topic
        self.content = content

    def info(self) :
        return f"<<mulac.{type(self).__name__} src = {self.src}, dest = {self.dest}, topic = {self.topic}, content = {self.content}>>"

def raw_to_msg(raw) :
    src, dest, topic = raw.split('/')[0:3]
    content = '/'.join(raw.split('/')[3:])
    return Message(src = src, dest = dest, topic = topic, content = content)

def msg_to_raw(msg) :
    return "%s/%s/%s/%s" % (msg.src, msg.dest, msg.topic, msg.content)

class Logger(object) :
    def __init__(self, path = None) :
        self.path = path
        self.logs = []

    def info(self) :
        return f"<<mulac.{type(self).__name__} path = {self.path}, len_logs = {len(self.logs)}>>"

    def log(self, line) :
        self.logs.append("[%s] %s" % (get_current_time(), line))

    def dump(self) :
        if len(self.logs) > 0 :
            if self.path is not None and os.path.isfile(self.path) :
                with open(self.path, 'a') as f :
                    f.writelines([line + "\n" for line in self.logs])
            else :
                for line in self.logs :
                    print(line)
            self.logs = []

class Agent(object) :
    def __init__(self, id) :
        self.id = str(id)

    def info(self) :
        return f"<<mulac.{type(self).__name__} id = {self.id}>>"

    def process(self, msgs) :
        result = {"msgs" : []}
        return result

class Backbone(object) :
    def __init__(self, path = None) :
        self.nodes = {}
        if path is not None :
            with open(path, 'r') as f :
                for node in json.load(f) :
                    self.add_node(in_id = str(node["in_id"]), in_end = node["in_end"],
                        out_id = str(node["out_id"]), out_end = node["out_end"],
                        nbs = node.get("nbs", []), group = node.get("group", []))

    def info(self) :
        return f"<<mulac.{type(self).__name__} num_nodes = {len(self.nodes)}>>"

    def add_node(self, in_id, in_end, out_id, out_end, nbs, group = None) :
        self.nodes[in_id] = {"in_end" : str(in_end), "out_id" : str(out_id), "out_end" : out_end,
            "nbs" : list(map(str, nbs)), "group" : list(map(str, group)) if group is not None else []}

    def find_node(self, current, dest, excludes = None) :
        node, next = None, None
        if excludes is None :
            excludes = []
        if dest == current or dest in self.nodes[current]["group"] :
            node = current
            next = current
        else :
            for nb in self.nodes[current]["nbs"] :
                if nb in excludes :
                    continue
                node, _ = self.find_node(current = nb, dest = dest, excludes = excludes + [current, ])
                if node is not None :
                    next = nb
                    break
        return node, next

class PubAgent(Agent) :
    def __init__(self, id, bb, timeout = 1) :
        super(PubAgent, self).__init__(id = id)
        self.bb, self.timeout = bb, timeout
        self.in_id = None
        self.pub = None
        for id, node in self.bb.nodes.items() :
            if self.id == node["out_id"] :
                self.pub = node["out_end"]
                self.in_id = id
        self.buffer = []

    def info(self) :
        return f"<<mulac.{type(self).__name__} pub = {self.pub}>>"

    def process(self, msgs) :
        result = {"msgs" : []}
        for msg in msgs :
            if msg.topic == "zero" :
                msg = self.validate_send(msg)
                if msg is not None :
                    self.buffer.append((msg_to_raw(msg), time.time()))
        if len(self.buffer) > 0 :
            context = zmq.Context()
            socket = context.socket(zmq.PUB)
            socket.bind(self.pub)
            msg, stamp = self.buffer.pop(0)
            if time.time() - stamp < self.timeout :
                repeat = 2
                while repeat > 0 :
                    socket.send_string(msg)
                    time.sleep(0.1)
                    repeat -= 1
            else :
                self.buffer = []
            socket.close()
            context.term()
        return result

    def validate_send(self, msg) :
        valid_msg = None
        content_msg = raw_to_msg(msg.content)
        node, next = self.bb.find_node(current = self.in_id, dest = content_msg.dest, excludes = [self.in_id, ])
        if next is not None :
            valid_msg = Message(src = self.id, dest = next, topic = "zero", content = msg.content)
        return valid_msg

class SubAgent(Agent) :
    def __init__(self, id, bb, timeout = 1) :
        super(SubAgent, self).__init__(id = id)
        self.bb, self.timeout = bb, timeout
        self.subs = []
        for id, node in self.bb.nodes.items() :
            if self.id in node["nbs"] :
                self.subs.append(node["out_end"])

    def info(self) :
        return f"<<mulac.{type(self).__name__} subs = {self.subs}>>"

    def process(self, msgs) :
        result = {"msgs" : []}
        if len(self.subs) > 0 :
            context = zmq.Context()
            socket = context.socket(zmq.SUB)
            for sub in self.subs :
                socket.connect(sub)
            socket.subscribe("")
            poller = zmq.Poller()
            poller.register(socket, zmq.POLLIN)
            if poller.poll(100) :
                raw = socket.recv_string()
                if len(raw) > 0 :
                    msg = self.validate_recv(raw_to_msg(raw))
                    if msg is not None :
                        result["msgs"].append(msg)
                    msg = self.validate_hop(raw_to_msg(raw))
                    if msg is not None :
                        result["msgs"].append(msg)
            socket.close()
            context.term()
        return result

    def validate_recv(self, msg) :
        valid_msg = None
        content_msg = raw_to_msg(msg.content)
        if msg.dest is None or msg.dest == self.id or content_msg.dest in self.bb.nodes[self.id]["group"] :
            valid_msg = Message(src = self.id, dest = content_msg.dest, topic = "zero", content = msg.content)
        return valid_msg

    def validate_hop(self, msg) :
        valid_msg = None
        content_msg = raw_to_msg(msg.content)
        node, next = self.bb.find_node(current = self.id, dest = content_msg.dest, excludes = [msg.src, self.id, ])
        if next is not None and next != self.id :
            valid_msg = Message(src = self.id, dest = self.bb.nodes[self.id]["out_id"], topic = "zero", content = msg.content)
        return valid_msg

def run_agent(agent, in_queue, out_queue, timeout = 60) :
    start_time = time.time()
    while time.time() - start_time < timeout :
        msgs = get_all_items_in_queue(in_queue)
        result = agent.process(msgs = msgs)
        out_msgs = result.get("msgs", [])
        if len(out_msgs) > 0 :
            put_items_to_queue(out_queue, items = out_msgs)
        agent.logger.dump()

class Monitor(object) :
    def __init__(self, akls, qkls, path = None) :
        self.akls = akls
        self.qkls = qkls
        self.logger = Logger(path = path)
        self.running = False

    def info(self) :
        return f"<<MCF.{type(self).__name__}>>"

    def run(self, agents, timeout = 60) :
        in_queues = {agent.id : self.qkls() for agent in agents}
        out_queues = {agent.id : self.qkls() for agent in agents}

        agent_pool = {agent.id : self.akls(target = run_agent, kwargs = {
            "agent" : agent, "in_queue": in_queues[agent.id], "out_queue" : out_queues[agent.id],
            "timeout" : timeout}) for agent in agents}

        for agent in agent_pool.values() :
            agent.start()

        start_time = time.time()
        finish_time = None
        while time.time() - start_time < timeout :
            for _, out_queue in out_queues.items() :
                msg = get_one_item_in_queue(out_queue)
                if msg is not None :
                    if msg.dest is None :
                        if msg.content is None :
                            timeout = 0
                            self.logger.log("monitor done.")
                        else :
                            self.handle_msg(msg = msg)
                    elif msg.dest == "" :
                        for dest in [agent.id for agent in agents if agent.id != msg.src] :
                            put_one_item_to_queue(in_queues[dest], msg)
                    else :
                        put_one_item_to_queue(in_queues[msg.dest], msg)

            self.logger.dump()
        finish_time = time.time() if finish_time is None else finish_time
        for agent in agent_pool.values() :
            if hasattr(agent, 'terminate') :
                agent.terminate()
            agent.join()
        return finish_time - start_time

    def handle_msg(self, msg) :
        if msg.topic == 'log' :
            self.logger.log('[log from %s] %s' % (msg.src, msg.content))


class ProcessMonitor(Monitor) :
    def __init__(self, path = None) :
        super(ProcessMonitor, self).__init__(akls = multiprocessing.Process,
                                             qkls = multiprocessing.Queue, path = path)

class ThreadMonitor(Monitor) :
    def __init__(self, path = None) :
        super(ThreadMonitor, self).__init__(akls = threading.Thread,
                                            qkls = queue.Queue, path = path)
