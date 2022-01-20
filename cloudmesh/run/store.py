import redis
import os
from subprocess import check_call
import subprocess
from cloudmesh.common.Shell import Shell
from multiprocessing import Pool

"""
files = Store(host="localhost", port=6379)
files.connect()
if files.count == 0:
    names = files.add("/data/dsc/us.sitesucker.mac.sitesucker")
    print (files.count)

"""


def _execute(i):
    try:
        i = str(i)
        store = Store(host="localhost", port=6379)
        r = store.connect()
        command = store.get(i, db=Store.TODO)
        print(i, command)
        os.system(command)
        store.set(i, command, db=Store.DONE)
        store.set(i, command, db=Store.TODO)
        store.delete(i, db=Store.TODO)
        print(i, "updated")
    except Exception as e:
        print(e)


class Scheduler:

    def __init__(self, store):
        self.store = store

    def run(self, from_id=1, to_id=None, parallelism=1):
        to_id = to_id or self.store.count(db=0)
        commands = []
        print("GGG", from_id, to_id)
        for i in range(from_id, to_id + 1):
            commands.append(i)
        print(commands)
        with Pool(parallelism) as p:
            p.map(_execute, commands)



class Store:
    TODO = 0
    DONE = 1
    FILES = 2
    DIRS = 3

    NAME = ["todo", "done", "files", "dirs"]

    def get_index(self, name):
        if type(name) == int:
            return name
        else:
            return self.NAME.index(name)

    def _run(self, command):
        # command_list = shlex.split(command)
        # return subprocess.check_output(command_list)
        return Shell.run(command)

    def print(self, db=0):
        print("ID\tVALUE")
        for key in self.r[db].scan_iter():
            value = self.r[db].get(key)
            print(f"{key}\t{value}")

    def __init__(self,
                 name="todo",
                 host="localhost",
                 port=6379,
                 directory=None):
        self.name = name
        self.host = host
        self.port = port
        self.container_id = None
        self.r = [None, None, None, None]
        if directory is None:
            self.directory = os.getcwd()
        else:
            self.directory = directory

    def connect(self):
        self.r = [None, None, None, None]
        for _id in [0, 1, 2, 3]:
            self.r[_id] = redis.Redis(host="localhost", port=6379, db=_id, decode_responses=True)
        # self.r[0].set("x", "y")



    def shell(self):
        os.system("docker exec -it {self.name} bash")

    def status(self, db=0):
        try:
            size = self.r[db].dbsize()
            running = True
        except:
            size = None
            running = False

        self.container_id = Shell.run(f'docker ps -aqf "name={self.name}"').strip() or None
        print(f"Name      : {self.name}")
        print(f"DB        : {db} {self.NAME[db]}")
        print(f"Host      : {self.host}")
        print(f"Port      : {self.port}")
        print(f"Directory : {self.directory}")
        print(f"Count     : {size}")
        print(f"Running   : {running}")
        print(f"ID        : {self.container_id}")
        for i in range(0, 4):
            size = self.elements(db=i)
            print(f"Size {self.NAME[i]:<5}: {size}")
        for i in range(0, 4):
            try:
                p = self.r[i].ping()
            except:
                p = False
            print(f"Ping {self.NAME[i]:<5}: {p}")
        #for i in range(0, 4):
        #    try:
        #        p = self.r[i].memory_stats()
        #    except:
        #        p = False
        #    print(f"Memory {self.NAME[i]:<5}: {p}")

    def start(self):
        command = f"docker run -d -p 127.0.0.1:{self.port}:6379" \
                  f" --name {self.name}" \
                  f" -v {self.directory}/redis-conf:/redis-conf redis redis-server /redis-conf"
        print(command)
        ret = Shell.run(command)
        print(ret)

    def stop(self):
        print(f"Stopping {self.name} ...")
        try:
            r1 = Shell.run(f"docker stop {self.name}")
            r2 = Shell.run(f"docker rm {self.name}")
            if "No such container:" in r1 or "No such container:" in r2:
                raise ValueError
        except:
            print(f"{self.name} is not running")
        print(f"Stopping {self.name} done")

    def set(self, key, value, db=0):
        print ("DD", key,  type(key), value, db, self.r[db], self.elements(db=db))
        self.r[db].set(str(key), value)
        print(self.elements(db=db))

    def get(self, key, db=0):
        try:
            return self.r[db].get(key)
        except:
            return None

    def delete(self, key, db=0):
        try:
            self.r[db].delete(key)
        except Exception as e:
            print(e)

    def flush(self, db=None):
        if db is None:
            ids = [0, 1, 2, 3]
        else:
            ids = [db]
        for i in ids:
            try:
                self.r[i].flushdb()
            except:
                pass

    def elements(self, db=0):
        try:
            return self.r[db].dbsize()
        except Exception as e:
            print (e)
            return 0

    def append(self, command, db=0):
        n = self.elements(db=db)
        n = n + 1
        self.set(n, command, db=db)
    """
	def add_directory(self, directory, progress=True):
		# create a list of file and sub directories
		# names in the given directory
		listOfFile = os.listdir(directory)
		allFiles = list()
		# Iterate over all the entries
		for entry in listOfFile:
			# Create full path
			fullPath = os.path.join(directory, entry)
			# If entry is a directory then get the list of files in this directory
			if os.path.isdir(fullPath):
				allFiles = allFiles + self.add_files(fullPath)
			else:
				allFiles.append(fullPath)
				self.counter = self.counter + 1
				if progress:
					print(self.counter, end="\r")
				self.set(self.counter, fullPath)

		return allFiles

	def create_tree(self, source, destination):
		# os.system(f"time run -P -zr -f\"+ */\" -f\"- *\" -e 'ssh -c aes128-ctr' {source}/ {destination}/")
		os.system(f"time run -P -zr -f\"+ */\" -f\"- *\" -e ssh  {source}/ {destination}/")

	
	def range(self, from_key, to_key):
		# only works if the key is int
		data = []
		for key in range(from_key, to_key + 1):
			value = str(self.r.get(key), "utf-8")
			data.append(value)
		return data

	def copy(files, remote):
		# remote = user@host:
		for file in files:
			where = remote + file.replace("/data/dsc/us.sitesucker.mac.sitesucker", "")

			directory = os.path.dirname(where).split(":")[1]
			if directory not in remote_dirs:
				command = ["ssh", cred, "mkdir", "-p", directory]
				print_command(command)
				try:
					r = check_call(command)
					remote_dirs.append(directory)
				except Exception as e:
					if "File exists":
						pass
					else:
						print("E", e)
			else:
				# print("*", directory, "exists")
				pass

			command = ["run", "-Pa", file, where]
			print_command(command)
			check_call(command)
	"""
