from cloudmesh.shell.command import command
from cloudmesh.shell.command import PluginCommand
from cloudmesh.common.console import Console
from cloudmesh.common.util import path_expand
from pprint import pprint
from cloudmesh.common.debug import VERBOSE
from cloudmesh.shell.command import map_parameters
from cloudmesh.common.util import readfile
import os


class RunCommand(PluginCommand):

    # noinspection PyUnusedLocal
    @command
    def do_run(self, args, arguments):
        """
        ::

          Usage:
                run start
                run stop
                run status
                run job get ID
                run job set ID COMMAND
                run job rm ID
                run job delete ID
                run job add COMMAND [--db=DB]
                run job add --file=FILE
                run cp add DIRECTORY [DESTINATION]
                run cp dirs find
                run cp dirs reduce
                run cp example [DIRECTORY]
                run view [--db=DB] [--from=FROM] [--to=TO]
                run todo
                run todo ID

          This command does some useful things.

          Arguments:
              FILE   a file name

          Options:
              -f      specify the file

          Description:
                run start
                    starts the database servcices using docker containers.
                    We use one for all files o be submitted
                    We use another one that holds all directories.

                run stop
                    stops the database services

                run status
                    prints the status and information about the database services

                run add DIRECTORY
                    adds the named directory to the files to be submitted. The files
                    will be added as absolute path in the file database

                run dirs find
                    finds all directories in the file database

                run dirs reduce
                    it is possible that the find function finds the same
                    directory multiple times, or even finds parent directories.
                    To optimize the number of directories created, we can reduce them with
                    this command.
                    The command run dirs find must be called before you invoke the
                    reduce command.

                run view [--db=DB] [--from=FROM] [--to=TO]
                    DB can have the values 'todo', 'files', 'done', 'dirs'

                run example [DIRECTORY]
                   creats an example directory. If the directory is not
                   specified the name "example" is used.

        """

        # arguments.FILE = arguments['--file'] or None

        map_parameters(arguments, "file")

        # VERBOSE(arguments)
        from cloudmesh.run.store import Store, Scheduler

        store = Store()
        r = store.connect()

        """
        def dir_list(filename):
            # TODO: we also need a path replace
            db = connect()
            size = len(db["dirs"])

            counter = 0
            with open(filename, "w") as mkdir_file:
                for key in dirDB.r.scan_iter():
                    counter = counter + 1
                    print(f"{counter}/{size}", end="\r")
                    key = str(key, "utf-8").replace("/data/dsc/us.sitesucker.mac.sitesucker/", "")
                    mkdir_file.write(f'{key}\n')


        def find_dirs():
            db = connect()
            last_dir = ""
            counter = 0
            for i in range(1, len(todoDB) + 1):
                # for i in range(1, 4):
                d = os.path.dirname(todoDB.get(i))
                if d != last_dir:
                    counter = counter + 1
                    print(counter, i, d)
                    if db["dirs"].get(d):
                        pass
                    else:
                        db["dirs"].set(d, "1")
                    last_dir = d

        def reduce_dirs():
            import PurePath
            db = connect()

            found_counter = 0
            counter = 0

            for key in db["dirs"].r.scan_iter():
                counter = counter + 1

                path = PurePath(str(key, "utf-8")).parts

                size = len(db["dirs"])
                print(f"{counter}/{size} {found_counter} {len(path)}           ", end="\r")

                #p = str.encode("/".join(path[0:len(path) + 1])[1:])

                for i in range(2, len(path)):
                    part = str.encode("/".join(path[0:i])[1:])
                    found = db["dirs"].get(part)
                    if found:
                        found_counter = found_counter + 1
                        db["dirs"].r.delete(part)
        """

        def start():
            store.start()
            status()

        def stop():
            # for database in db:
            #    db[database].stop()
            store.stop()
            status()

        def status():
            # for database in db:
            #   print (79 * "=")
            #   db[database].status()
            print(79 * "=")
            store.status()

            print(79 * "=")
            os.system('docker ps --format "table {{.Names}}\t{{.ID}}\t{{.Status}}\t{{.Ports}}"')
            print(79 * "=")

        if arguments.start:
            start()

        elif arguments.stop:
            stop()

        elif arguments.status:
            status()

        elif arguments.find and arguments.dirs and arguments.cp:
            find_dirs()

        elif arguments.job and arguments.get and arguments.ID:
            db = arguments["--db"] or "todo"
            _id = store.get_index(name=db)
            value = store.get(arguments.ID, db=_id)
            print(value)

        elif arguments.job and arguments.add and arguments.COMMAND:

            db_name = arguments["--db"] or "todo"
            db = store.get_index(db_name)
            store.append(arguments.COMMAND, db=db)
            # store.flush(db=db)

        elif arguments.job and arguments.add and arguments["--file"]:
            filename = arguments["--file"]
            data = readfile(filename).strip().splitlines()
            for line in data:
                store.add(line, db=Store.TODO)

        elif arguments.reduce and arguments.dirs and arguments.cp:
            reduce_dirs()

        elif arguments.view:

            store.print(db=0)
            """
            database = arguments["--db"] or "todo"
            id = Store.NAME.index(database)
            print ("PPP", id)
            count = store.count(db=id)

            from_id = int(arguments["--from"] or 1)
            to_id = int(arguments["--to"] or count)
            if count == 0:
                print(f"Database '{database}' is empty")
            elif to_id > count:
                to_id = count
            elif to_id < from_id:
                print("FROM must be smaller then TO")
            else:
                #print (f"Range: {from_id}, {to_id}")
                print ("ID\tCOMMAND")
                for i in range(from_id, to_id + 1):
                    value = store.get(str(i), db=id)
                    if value is None:
                        value = "None"
                    print(f"{i}\t{value}")
            """

        elif arguments.example and arguments.cp:
            name = arguments["DIRECTORY"] or "example"
            from cloudmesh.run.example import Example
            e = Example(name)
            e.create()

        elif arguments.todo and arguments.ID:
            _id = int(arguments.ID)
            scheduler = Scheduler(store)
            scheduler.run(_id, _id, parallelism=1)

        elif arguments.todo:
            scheduler = Scheduler(store)
            scheduler.run(parallelism=1)

        elif arguments.job and (arguments.rm or arguments.delete):
            try:
                store.delete(arguments.ID, db=0)
            except:
                print(f"Job {arguments.ID} can not be deleted")

        elif arguments.job and arguments.set:
            _id = arguments.ID
            command = arguments.COMMAND
            store.set(_id, command, db=0)

        elif arguments.cp and arguments.add and arguments.DIRECTORY:
            destination = arguments.DESTINATION
            source = arguments.DIRECTORY
            store.add_directory(source)
            print()

        return ""
