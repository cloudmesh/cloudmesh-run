import os
import textwrap


class Example:

    def __init__(self, name):
        self.data = textwrap.dedent(
            f"""
            {name}/1.txt
            {name}/2.txt
            {name}/3.txt
            {name}/4.txt
            {name}/a/5.txt
            {name}/a/6.txt
            {name}/a/7.txt
            {name}/a/8.txt
            """).strip().splitlines()

    def create(self):
        for name in self.data:
            print(f"create {name}")
            d = os.path.dirname(name)
            n = os.path.basename(name)
            if not os.path.isdir(d):
                os.system(f"mkdir -p {d}")
            os.system(f"touch {name}")
