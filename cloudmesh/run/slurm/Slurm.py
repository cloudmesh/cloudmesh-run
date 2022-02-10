from cloudmesh.common.util import path_expand

class Slurm:

    def __init__(self,
                 host=None,
                 user=None,
                 queue=None,
                 pubkey="~/.ssh/id_rsa.pub"):
        self.host = host
        self.user = user
        self.queue = queue
        self.pubkey = path_expand(pubkey)

        # if self.host in ssh/config:
        #   read hostname and user form config file
        #   see cloudmesh.common for sshconfig management prg

        # see also the old cloudmesh-experiment

    def set_queue(self, name):
        """
        Sets the queue name

        :param name: the queue name
        :type name: str
        """
        self.queue = name

    def run(self, name=None, job=None, queue=None):
        """
        Run the job directly via srun

        :param name: The name of the job
        :type name: str
        :param job: the name of the job script
        :type job: str
        :param queue: overwrites the default queue
        :type queue: str
        :return: jobid
        :rtype: str
        """
        pass

    def batch(self, name, job, queue=None):
        """
        Submit the job to the queue with sbatch

        :param name: The name of the job
        :type name: str
        :param job: the name of the job script
        :type job: str
        :param queue: overwrites the default queue
        :type queue: str
        :return: jobid
        :rtype: str
        """
        pass

    def list(self, query=None):
        """
        List all jobs on the machine with squeue(?). The return can be restricted
        with a query

        :param query:
        :type query:
        :return: all jobs queried
        :rtype: dict
        """
        pass

    def info(self):
        """
        Returns information about the queue

        :return: information about the queue
        :rtype: dict
        """
        pass

    # we need to make sure we can specify the queu name for submission

    # we want local database with job ids
