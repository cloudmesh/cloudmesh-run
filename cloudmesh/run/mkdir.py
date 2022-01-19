
import os
import sys
from multiprocessing import Pool
import multiprocessing

class MkDir:

    def init(self):
        self.sounter = 0

    def mkdir(name):
        self.counter = self.counter + 1
        name = name.strip()
        if not os.path.isdir(name):
            os.system(f'mkdir -p "{name}"')
            print (f"+{self.counter}", end="\r")
        else:
            print (f"-{self.counter}", end="\r")

    def create_from_file(self, name):
        self.counter=0
        with open(name, "r") as f:
            lines = f.readlines()

        print (f" Create {len(lines)} directories")

        n = multiprocessing.cpu_count() - 1
        with Pool(n) as p:
            p.map(self.mkdir, lines)

