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
		store = Store(host="localhost", port=6379)
		r = store.connect()
		todo = r[Store.TODO]
		command = todo.get(i)
		print(i, command)
		os.system(command)
		# done.set(i, command)
		todo.delete(i)
		print(i, "updated")
	except Exception as e:
		print (e)

class Scheduler:

	def __init__(self, todo=None, done=None):
		self.todo = todo
		self.done = done

	def run(self, from_id, to_id, parallism=1):

		commands = []
		for i in range(from_id, to_id+1):
			commands.append(i)
		print (commands)
		with Pool(parallism) as p:
			p.map(_execute, commands)

class Store:

	TODO = 0
	DONE = 1
	FILES = 2
	DIRS = 3

	NAME = ["todo",
			"done",
			"files",
			"dirs"
			]

	def get_index(self, name):
		if type(name) == int:
			return name
		else:
			return self.NAME.index(name)

	def _run(self, command):
		#command_list = shlex.split(command)
		#return subprocess.check_output(command_list)
		return Shell.run(command)

	def __init__(self,
				 name="todo",
				 host="localhost",
				 port=6379,
				 directory=None):
		self.name = name
		self.host = host
		self.port = port
		self.container_id= None
		self.r = {}
		if directory == None:
			self.directory = os.getcwd()
		else:
			self.directory = directory

	def connect(self):
		self.r = {}
		for id in [0,1,2,3]:
			self.r[id] = redis.Redis(host="localhost", port=6379, db=id)

		return self.r

	def count(self, db=0):
		try:
			return self.r[db].dbsize()
		except:
			return 0

	def shell(self):
		os.system ("docker exec -it {self.name} bash")

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
		for i in range(0,4):
			size = self.count(i)
			print(f"Size {self.NAME[i]:<5}: {size}")

	def start(self):
		command = f"docker run -d -p 127.0.0.1:{self.port}:6379" \
				  f" --name {self.name}" \
				  f" -v {self.directory}/redis-conf:/redis-conf redis redis-server /redis-conf"
		print(command)
		r = Shell.run(command)
		print(r)


	def stop(self):
		print(f"Stopping {self.name} ...")
		try:
			r1 = Shell.run(f"docker stop {self.name}")
			r2 = Shell.run(f"docker rm {self.name}")
			if "No such container:" in r1 or "No such container:" in r2:
				raise ValueError
		except:
			print (f"{self.name} is not running")
		print(f"Stopping {self.name} done")

	def reset(self):
		pass

	def set(self, key, value, db=0):
		db = self.get_index(db)
		self.r[db].set(key, value)

	def get(self, key, db=0):
		db = self.get_index(db)
		try:
			return str(self.r[db].get(key), "utf-8")
		except:
			return None

	def delete(self, key, db=0):
		db = self.get_index(db)
		try:
			self.r[db].delete(key)
		except Exception as e:
			print (e)

	def add_job(self, command):
		return self.add(command, db=0)

	def add(self, command, db=0):
		db = self.get_index(db)
		n = self.count(db=db)
		n = n + 1
		self.set(n, command, db=db)
		return n

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


