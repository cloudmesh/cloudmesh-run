import os
from multiprocessing import Pool
import multiprocessing


class MkDir:

    def init(self):
        self.sounter = 0

    def mkdir(self, name):
        self.counter = self.counter + 1
        directory = name.strip()
        if not os.path.isdir(directory):
            os.system(f'mkdir -p "{directory}"')
            print(f"+{self.counter}", end="\r")
        else:
            print(f"-{self.counter}", end="\r")

    def create_from_file(self, name):
        self.counter = 0
        with open(name, "r") as f:
            lines = f.readlines()

        print(f" Create {len(lines)} directories")

        n = multiprocessing.cpu_count() - 1
        with Pool(n) as p:
            p.map(self.mkdir, lines)
