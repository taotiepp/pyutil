#coding=utf8
import os
import time
import fcntl
import uuid
try:
    import cPickle as pickle
except ImportError:
    import pickle

queue_path = 'queue'


class FLOCK(object):
    def __init__(self, name):
        self.fobj = open(name, 'w')
        self.fd = self.fobj.fileno()

    def lock(self):
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX)
            return True
        except Exception, e:
            print e
            return False

    def unlock(self):
        fcntl.flock(self.fd, fcntl.LOCK_UN)


class TaskQueue(object):
    def __init__(self, queue_name=str(uuid.uuid1())):
        self.queue_path = os.path.join(queue_path, queue_name)
        if not os.path.exists(self.queue_path):
            os.makedirs(self.queue_path)
        self.lock_file = "%s.%s" % (self.queue_path, 'lock')
        self.max_id_file = "%s.id" % (self.queue_path)
        self.locker = FLOCK(self.lock_file)
        self.queue_len = 0
        self.__load_queue_info()

    def lock_decrator(func):
        def _wrapper(self, *args, **kargs):
            try:
                self.locker.lock()
                ret = func(self, *args, **kargs)
                self.locker.unlock()
            except Exception,e:
                self.locker.unlock()
                raise e
            return ret
        return _wrapper

    @lock_decrator
    def __load_queue_info(self):
        queue = self.list()
        self.queue_len = len(queue)
        if len(queue) > 0:
            queue.sort(reverse=True)
            self.max_id = queue[0]
        else:
            self.max_id = 0

    def __get_max_id(self):
        with open(self.max_id_file) as f:
            max_id = f.read()
            max_id = int(max_id)
            return max_id

    @lock_decrator
    def list(self):
        files = os.listdir(self.queue_path)
        queue = []
        for i in files:
            queue.append(int(os.path.basename(i)))
        return queue

    @lock_decrator
    def push(self, obj):
        self.queue_len += 1
        self.max_id += 1
        fPath = os.path.join(self.queue_path, str(self.max_id))
        with open(fPath, 'w') as f:
            pickle.dump(obj, f)
            f.flush()

    @lock_decrator
    def pop_nowait(self):
        files = self.list()
        ret = None
        if len(files) > 0:
            files.sort(reverse=True)
            fPath = os.path.join(self.queue_path, str(files[0]))
            with open(fPath) as f:
                ret = pickle.load(f)
            os.remove(fPath)
            self.queue_len -= 1
        return ret


    def pop(self):
        while True:
            if self.queue_len > 0:
                return self.pop_nowait()
            time.sleep(1)

    def len(self):
        return self.queue_len
        pass


if __name__ == '__main__':
    myquque = TaskQueue('test')
    myquque.push({'hahahhahah':'fdasfsdfa'})
    myquque.push('hahahhahah')
    myquque.push('hegegegeg')
    print 1,myquque.list()
    print 2,myquque.list()
    while myquque.len() > 0:
        print myquque.pop()
    print 3,myquque.list()
   
