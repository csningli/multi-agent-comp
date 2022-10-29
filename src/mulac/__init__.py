
import os, time, itertools, datetime
import queue, multiprocessing, threading, zmq

def get_current_time() :
    return datetime.datetime.now().strftime("%Y/%m/%d_%H:%M:%S.%f")

def get_datetime_stamp() :
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S_%f")

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
    src, dest, topic = raw.split('|')[0:3]
    content = '|'.join(raw.split('|')[3:])
    return Message(src = src, dest = dest, topic = topic, content = content)

def msg_to_raw(msg) :
    return "%s|%s|%s|%s" % (msg.src, msg.dest, msg.topic, msg.content)

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
            if self.path is not None :
                with open(self.path, 'a') as f :
                    f.writelines([line + "\n" for line in self.logs])
            else :
                for line in self.logs :
                    print(line)
            self.logs = []

class Agent(object) :
    def __init__(self, id, pub = None, sub = None) :
        self.id, self.pub, self.sub = str(id), pub, sub

    def info(self) :
        return f"<<mulac.{type(self).__name__} id = {self.id}>>"

    def process(self, msgs) :
        result = {"msgs" : []}
        return result

def run_agent(agent, in_queue, out_queue, timeout = 60) :
    start_time = time.time()
    while time.time() - start_time < timeout :
        msgs = get_all_items_in_queue(in_queue)
        result = agent.process(msgs = msgs)
        out_msgs = result.get("msgs", [])
        if len(out_msgs) > 0 :
            put_items_to_queue(out_queue, items = out_msgs)

def run_pub(address, in_queue, out_queue, timeout = 60) :
    start_time = time.time()
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(address)
    while time.time() - start_time < timeout :
        msgs = get_all_items_in_queue(in_queue)
        for msg in msgs :
            socket.send_string(msg_to_raw(msg))
    socket.close()
    context.term()

def run_sub(address, in_queue, out_queue, timeout = 60) :
    start_time = time.time()
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(address)
    socket.subscribe("")
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    while time.time() - start_time < timeout :
        msgs = []
        if poller.poll(100) :
            raw = socket.recv_string()
            msgs.append(raw_to_msg(raw))
        put_items_to_queue(out_queue, items = msgs)
    socket.close()
    context.term()

class Monitor(object) :
    def __init__(self, tkls, qkls, path = None) :
        self.tkls = tkls
        self.qkls = qkls
        self.logger = Logger(path = path)
        self.running = False

    def info(self) :
        return f"<<mulac.{type(self).__name__}>>"

    def run(self, agents, timeout = 60) :
        in_queues, out_queues, task_pool = {}, {}, {}
        sub_agents = {}
        for agent in agents :
            for key, qkls, tkls, first, target in [
                (agent.id, self.qkls, self.tkls, agent, run_agent),
                (agent.pub, self.qkls, self.tkls, agent.pub, run_pub),
                (agent.sub, self.qkls, self.tkls, agent.sub, run_sub),
            ] :
                if key is not None :
                    if key not in in_queues.keys() :
                        in_queues[key] = self.qkls()
                    if key not in out_queues.keys() :
                        out_queues[key] = self.qkls()
                    if key not in task_pool.keys() :
                        task_pool[key] = self.tkls(target = target, args = [
                            first, in_queues[key], out_queues[key], timeout])
            if agent.sub is not None :
                if agent.sub not in sub_agents.keys() :
                    sub_agents[agent.sub] = []
                sub_agents[agent.sub].append(agent.id)

        for task in task_pool.values() :
            task.start()

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
                        if msg.dest in sub_agents.keys() :
                            for agent_id in sub_agents[msg.dest] :
                                put_one_item_to_queue(in_queues[agent_id], msg)
                        if msg.dest in in_queues.keys() :
                            put_one_item_to_queue(in_queues[msg.dest], msg)

            self.logger.dump()
        finish_time = time.time() if finish_time is None else finish_time
        for task in task_pool.values() :
            if hasattr(task, 'terminate') :
                task.terminate()
            task.join()
        return finish_time - start_time

    def handle_msg(self, msg) :
        if msg.topic == 'log' :
            self.logger.log('[log from %s] %s' % (msg.src, msg.content))


class ProcessMonitor(Monitor) :
    def __init__(self, path = None) :
        super(ProcessMonitor, self).__init__(tkls = multiprocessing.Process,
                                             qkls = multiprocessing.Queue, path = path)

class ThreadMonitor(Monitor) :
    def __init__(self, path = None) :
        super(ThreadMonitor, self).__init__(tkls = threading.Thread,
                                            qkls = queue.Queue, path = path)
