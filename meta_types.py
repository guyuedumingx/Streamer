from copy import deepcopy
import datetime
import json
from threading import Thread, Lock

class Dataset():
    """
    记录
    keys: 标题数组
    values: 值数组
    keys: ['name', 'no', 'time']
    data: [
        ['zhangsan', '1', '2023-09-10'],
        ['lisi', '2', '2023-09-11'],
        ...
    ]
    """
    def __init__(self):
        self.keys = []
        self.data = []

        # 分流器集流器分片长度
        self.pieceLen = 1
        # 当前是第几片
        self.pieceIdx = 1
        # 数据流的哈希标识
        self.pieceHash = 0

    def isDict(self):
        return isinstance(self.data, dict)

    def isList(self):
        return isinstance(self.data, list)

    def isStr(self):
        return isinstance(self.data, str)

    def copy(self):
        return deepcopy(self)
    
    def merge(self, ix):
        self.data = self.data + ix.data
        return self
        

class StreamMethod():
    """
    处理流程：
                  -> lrun 对data是列表时的操作
    _handle -> run -> srun 对data是str时的操作           -> _run_next 触发后续流操作
                  -> drun 对dataset.data是dict时的操作
    """
    def __init__(self, use_deepcopy=False, use_thread=False, main_join=False):
        self.nextStreamList = []
        self.use_deepcopy = use_deepcopy
        self.use_thread = use_thread
        self.main_join = main_join
    
    def run(self, dataset):
        # 默认会根据data的类型来执行对应的方法
        if(dataset.isList()): dataset = self.lrun(dataset)
        elif(dataset.isDict()): dataset = self.drun(dataset)
        elif(dataset.isStr()): dataset = self.srun(dataset)
        return dataset

    def lrun(self, dataset):
        # run for list
        return dataset 
    
    def drun(self, dataset):
        # run for dict
        return dataset
    
    def srun(self, dataset):
        # run for str
        return dataset

    def _handle(self, dataset):
        dataset = self.run(dataset)
        self._run_next(dataset)
    
    def _run_next(self, dataset):
        need_thread_list = []
        uneed = []

        for method in self.nextStreamList:
            if(method.use_thread): need_thread_list.append(method)
            else: uneed.append(method)

        for method in need_thread_list:
            data = dataset
            if(method.use_deepcopy or len(self.nextStreamList) > 1):
                data = data.copy()
                data.pieceHash = hex(id(method))
            th = Thread(target=method._handle, args=(data,))
            th.start()
            if(self.main_join): th.join()

        for method in uneed:
            data = dataset
            if(method.use_deepcopy or len(self.nextStreamList) > 1):
                data = data.copy()
                data.pieceHash = hex(id(method))
            method._handle(data)

    def next(self, streamMethod):
        self.nextStreamList.append(streamMethod)

    def start(self):
        self._handle(Dataset())

    def copy(self):
        return deepcopy(self)


class StreamModule(StreamMethod):
    """
    流模块，可以将多个StreamMethod组合包装成一个
    侵入式，会改变输入数据
    """
    def __init__(self, stream=[], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream = stream
        self.outerMethod = StreamMethod()
    
    def _handle(self, dataset):
        if(len(self.stream) <= 0): return dataset
        self.outerMethod.nextStreamList = self.nextStreamList
        if(len(self.stream) > 1):
            for i in range(0, len(self.stream)-1):
                self.stream[i].next(self.stream[i+1])
        self.stream[-1].next(self.outerMethod)
        self.stream[0]._handle(dataset)


class MonitorModule(StreamMethod):
    """
    监控模块，非侵入式模块
    流过该模块的数据不会被改变
    """
    def __init__(self, stream=[], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stream = stream
    
    def run(self, dataset):
        if(len(self.stream) <= 0): return dataset
        if(len(self.stream) > 1):
            for i in range(0, len(self.stream)-1):
                self.stream[i].next(self.stream[i+1])
        self.stream[0]._handle(dataset.copy())
        return dataset


class Reader(StreamMethod):
    """
    数据输入输出流
    merge_func: 输入流并非一定是流的起点，当它不是流的起点时，可传入
    指定的merge_func来控制数据流的合并行为
    ix: inDataset
    cx: curDataset
    """
    def __init__(self, file_path='', encoding='utf-8', 
                 mode='r', file_suffix='', merge_func=lambda ix,cx : cx, 
                 readAsStr=False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.encoding = encoding
        self.mode = mode
        self.merge_func = merge_func
        self.file_suffix = file_suffix
        self.readAsStr = readAsStr

    def _handle(self, ix):
        # 数据合并,默认忽略输入的数据,可以传入指定的merge_func来控制合并方式
        cx = self.run(ix)
        dataset = self.merge_func(ix, cx)
        self._run_next(dataset)
    
    def run(self, dataset):
        res = Dataset()
        with open(self.file_path, mode=self.mode, encoding=self.encoding) as f:
            if(self.readAsStr): res.data = f.read()
            else: res.data = f.readlines()
        return res 

    
class Outer(StreamMethod):
    """
    输出流
    """
    def __init__(self, file_path='', mode='w', encoding='utf-8', file_suffix='', 
                use_datetime_suffix=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.file_suffix = file_suffix
        time = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")   
        if(self.file_path == ''):
            self.file_path = time + self.file_suffix
        elif(use_datetime_suffix):
            self.file_path = self.file_path.split(".")[0] + time + self.file_path.split(".")[-1]
        self.mode = mode
        self.encoding = encoding
    

class Printer():

    def __init__(self):
        self.globalMethods = {}
        self.no = 1
    
    def show(self, method, indent=0):
        if(method not in self.globalMethods):
            print("|--" * indent, end='')
            print(method, end = " |"+str(self.no) + "|\n")
            self.globalMethods[method] = {"no": self.no, "indent": indent}
            self.no += 1
        for next in method.nextStreamList:
            self.show(next, indent+1)


class StrStream():
    def __init__(self, method_lst=[], stream_lst=[], method_dict={}):
        self.method_lst = method_lst
        self.method_dict = {self.to_camel_case(item.__class__.__name__):item for item in method_lst}
        self.method_dict.update(method_dict)
        self.stream_lst = stream_lst
        self.start_method = None

    def to_camel_case(self, name):
        lst = list(name)
        lst[0] = lst[0].lower()
        return ''.join(lst)

    def addStream(self, *stream_str):
        self.stream_lst = self.stream_lst + stream_str
    
    def run(self):
        for idx, s in enumerate(self.stream_lst):
            method_lst = [self.method_dict[i.strip()] for i in s.split(".")]
            if(idx == 0): self.start_method = method_lst[0]
            for i in range(0, len(method_lst)-1):
                method_lst[i].next(method_lst[i+1])
        self.start_method.start()
