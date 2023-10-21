from meta_types import *


class Filter(StreamMethod):
    """
    根据传入的func函数来过滤dataset
    """
    def __init__(self, func, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.func = func
    
    def lrun(self, dataset):
        res = []
        for line in dataset.data:
            if(self.func(line)): res.append(line)
        dataset.data = res
        return dataset
    
    def drun(self, dataset):
        res = {}
        for k, v in dataset.data.items():
            if(self.func(k, v)): res[k] = v
        dataset.data = res 
        return dataset


class Map(StreamMethod):
    """
    对dataset的每一项使用f函数
    """
    def __init__(self, func, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.func = func
    
    def lrun(self, dataset):
        res = []
        for line in dataset.data:
            res.append(self.func(line))
        dataset.data = res 
        return dataset
    
    def drun(self, dataset):
        res = {}
        for key, value in dataset.data.items():
            res[key] = self.func(key, value)
        dataset.data = res 
        return dataset


class ItemMap(StreamMethod):
    """
    对dataset的每一项中的某个字段使用func函数
    """
    def __init__(self, idxOrkey, func, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.func = func
        self.idxOrkey = idxOrkey
    
    def run(self, dataset):
        for line in dataset.data:
            line[self.idxOrkey] = self.func(line[self.idxOrkey])
        return dataset


class ExchangeItemType(StreamMethod):
    """
    把dataset的每一项reshape成dict
    """
    def run(self, dataset):
        if(len(dataset.data) <= 0): return dataset
        if(isinstance(dataset.data[0], list)): dataset = self.lrun(dataset)
        elif(isinstance(dataset.data[0], dict)): dataset = self.drun(dataset)
        return dataset

    def lrun(self, dataset):
        res = []
        for line in dataset.data:
            obj = {}
            for i in range(0, len(dataset.keys)):
                obj[dataset.keys[i]] = line[i]
            res.append(obj)
        dataset.data = res
        return dataset
    
    def drun(self, dataset):
        res = []
        for line in dataset.data:
            l = []
            for key in dataset.keys:
                l.append(line[key])
            res.append(l)
        dataset.data = res
        return dataset


class Reshape(StreamMethod):
    """
    改变流的形状，即改变dataset.data
    """
    def __init__(self, func, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.func = func

    def run(self, dataset):
        return self.func(dataset)


class Prt(StreamMethod):
    """
    打印流数据
    """
    def __init__(self, prtKeys=True, sep='', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prtKeys = prtKeys
        self.sep = sep

    def lrun(self, dataset):
        if(self.prtKeys):
            print(dataset.keys)
        for line in dataset.data:
            print(line)
        return dataset
    
    def drun(self, dataset):
        print(dataset.data)
        return dataset


class Head(StreamMethod):
    """
    返回流的前n行数据
    """
    def __init__(self, n=5, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.n = n
    
    def lrun(self, dataset):
        dataset.data = dataset.data[0:self.n]
        return dataset


class Tail(StreamMethod):
    """
    返回流的后n行数据
    """
    def __init__(self, n=5, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.n = n
    
    def lrun(self, dataset):
        dataset.data = dataset.data[-self.n:]
        return dataset 


class Skip(StreamMethod):
    """
    跳过前n行数据
    """
    def __init__(self, n=5, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.n = n
    
    def lrun(self, dataset):
        dataset.data = dataset.data[self.n:]
        return dataset 


class Merger(StreamMethod):
    """
    集流器
    集合多个流的数据并统一输出
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tmpDatasets = {}
        self._lock = Lock()
    
    def _handle(self, dataset):
        self._lock.acquire()
        if(dataset.pieceHash not in self.tmpDatasets):
            self.tmpDatasets[dataset.pieceHash] = []
        self.tmpDatasets[dataset.pieceHash].append(dataset) 
        self._lock.release()
        # 等待所有的流都执行完成，触发集流器集流处理
        if(dataset.pieceLen == len(self.tmpDatasets[dataset.pieceHash])):
            dataset = self.run(self.tmpDatasets[dataset.pieceHash])
            # 重置该管道
            self.tmpDatasets[dataset.pieceHash] = []
            self._run_next(dataset)
    
    def run(self, datasets):
        data = []
        for dataset in datasets:
            data = data + dataset.data
        res_dataset = Dataset()
        res_dataset.keys = datasets[0].keys
        res_dataset.data = data
        return res_dataset


class Spliter(StreamMethod):
    """
    分流器
    把一个数据流分成多个数据流
    """
    def __init__(self, idxOrkey, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.idxOrkey = idxOrkey
        self.splitDict = {}

    def _handle(self, dataset):
        datasets = self.run(dataset)
        for data in datasets:
            self._run_next(data)

    def run(self, dataset):
        for line in dataset.data:
            key = line[self.idxOrkey]
            if(key not in self.splitDict):
                self.splitDict[key] = []
            self.splitDict[key].append(line)
        datasets = []
        pieceLen = len(self.splitDict)
        idx = 1 
        pieceHash = hex(id(datasets))
        for key, value in self.splitDict.items():
            dset = Dataset()
            dset.keys = dataset.keys
            dset.data = value

            # 标记子流元信息
            dset.pieceHash = pieceHash
            dset.pieceLen = pieceLen
            dset.pieceIdx = idx
            idx += 1

            datasets.append(dset)
        return datasets


class GroupBy(Spliter):
    pass


class CsvReader(Reader):
    """
    读入CSV数据
    dataset.keys对应csv的第一行 dataset.data是2:n行的二维数组
    """
    def __init__(self, file_path, encoding='gbk', skip=0, *args, **kwargs):
        super().__init__(file_path=file_path, encoding=encoding, *args, **kwargs)
        self.skip = skip

    def run(self, dataset):
        res = super().run(dataset)
        res.data = res.data[self.skip:]
        if(len(res.data) == 0): return dataset
        res.keys = res.data[0].split(",")
        if(len(res.data) <= 1): return dataset 
        lst = []
        for line in res.data[1:]:
            lst.append(line.split(","))
        res.data = lst
        return res 


class JsonReader(Reader):
    """
    JSON输入流
    """
    def __init__(self, file_path, *args, **kwargs):
        super().__init__(file_path=file_path, file_suffix='.json', readAsStr=True, *args, **kwargs)
    
    def run(self, dataset):
        indataset = super().run(dataset)
        obj = json.loads(indataset.data)
        for key, value in obj.items():
            setattr(indataset, key, value)
        return indataset


class CsvOuter(Outer):
    """
    CSV输出流
    """
    def __init__(self, *args, **kwargs):
        super().__init__(file_suffix='.csv', *args, **kwargs)
    
    def lrun(self, dataset):
        with open(self.file_path, self.mode, encoding=self.encoding) as f:
            f.write(",".join(dataset.keys))
            for line in dataset.data:
                f.write(",".join(line))
        return dataset
    
    def drun(self, dataset):
        with open(self.file_path, self.mode, encoding=self.encoding) as f:
            f.write(",".join(dataset.keys))
            res = []
            for key in dataset.keys:
                res.append(dataset.data[key])
            f.write(",".join(res))
        return dataset


class JsonOuter(Outer):
    """
    JSON输出流
    """
    def __init__(self, indent=2, *args, **kwargs):
        super().__init__(file_suffix='.json', *args, **kwargs)
        self.indent = indent
    
    def run(self, dataset):
        with open(self.file_path, encoding=self.encoding, mode=self.mode) as f:
            f.write(json.dumps(dataset.__dict__, ensure_ascii=False, indent=self.indent))
        return dataset