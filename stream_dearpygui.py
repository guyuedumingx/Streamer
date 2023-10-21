from guietta import _, Gui, Quit, HSeparator, E, L
from methods import *

class SimpleGui(StreamMethod):

    def __init__(self, **args):
        super().__init__(use_thread=False, **args)
    
    def lrun(self, dataset):
        #去除两端空格
        data = dataset.copy() 
        res = []
        for line in data.data:
            res.append([E(i.strip()) for i in line])
        data = [[i.strip() for i in data.keys],
                [ HSeparator ],
                *res]
        gui = Gui(*data)
        with open('style.css','r', encoding='utf-8') as f:
            qss = f.read()
        gui._app.setStyleSheet(qss)
        # gui.window().setWindowTitle('Google')
        gui.run()
        dataset.data = data
        return dataset

