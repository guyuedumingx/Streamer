from methods import *
from meta_types import Printer
from stream_dearpygui import SimpleGui

csvReader = CsvReader('test.csv')
csvOuter = CsvOuter()
jsonOuter = JsonOuter()
jsonReader = JsonReader('2023-10-20_12-33-18.json')


head = Head()
tail = Tail()
prt = Prt()
skip = Skip(2)
exchangeItemType = ExchangeItemType()


filter = Filter(lambda x: x[-2] != '' and x[-1] == '异常' or x[-3] == '异常')
groupByName = GroupBy(1)
merger = Merger()

workDayMap = ItemMap(2, lambda x: "休息日" if x == "星期日" or x == "星期六" else "工作日")
dayFormatMap = ItemMap(3, lambda x: "-".join(x.split("/")))
reday = ItemMap(2, lambda x: x[2])

printer = Printer()
simpleGui = SimpleGui()

monitor = MonitorModule([Head(), SimpleGui()])
smodule = StreamModule([filter, dayFormatMap, skip])
stream = StreamModule([csvReader, smodule, monitor, head, groupByName, simpleGui, prt])
stream.start()
