#!/usr/bin/env python
#!pip install paramiko

"""Example extractor based on the clowder code."""

import logging
import paramiko
import time as timer

import pyclowder
from pyclowder.extractors import Extractor


class SubmitJob(Extractor):
    """Submit a job to HPC cluster"""
    def __init__(self):
        Extractor.__init__(self)
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def process_message(self, connector, host, secret_key, resource, parameters):
        # Process the file and upload the results

        logger = logging.getLogger(__name__)
        inputfile = resource["local_paths"][0]
        file_id = resource['id']
        dataset_id = resource["parent"]["id"]
        print(parameters.get("parameters"))

        # SSH connection details
        hostname = 'cc-login.campuscluster.illinois.edu'  # the hostname of the cluster you want to run job
        port = 22  # Default SSH port
        username = ""  # username on the cluster
        #password = ""
        private_key = paramiko.RSAKey(filename="private_key")

        # Use the following if it's not an RSA key
        #private_key = paramiko.pkey.PKey.from_path(pkey_path="<private_key_file_path>")
        # command to submit the slurm job
        command = "sbatch run_umbs.bat"

        # Create an SSH client
        ssh_client = paramiko.SSHClient()

        # Automatically add the server's host key (this is insecure and should only be used for testing)
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Connect to the SSH server
            #ssh_client.connect(hostname, port, username, password)
            ssh_client.connect(hostname, port, username, pkey=private_key)

            # Perform actions on the cluster (e.g., execute commands)
            stdin, stdout, stderr = ssh_client.exec_command(command)
            result = stdout.read().decode()

            # Print the output of the command
            print("Output:")
            #print(result)
            job_id = result.split()[3]

            # Check the job status periodically
            while True:
                _, stdo, stde = ssh_client.exec_command(f"squeue -u {username} -j {job_id}")
                job_status = stdo.read().decode()
                print(job_status)
                connector.message_process(resource, job_status)

                # Break the loop if the job is completed or failed
                if job_id not in job_status:
                    break

                # Wait for a few seconds before checking again
                timer.sleep(60) # make it environmental variable

            sftp = ssh_client.open_sftp()
            # filepath of output file generated by the slurm job on the cluster
            remote_file_path = f"/home/{username}/openmp_umbs.o{job_id}"
            local_file_path = f"openmp_umbs.o{job_id}"
            sftp.get(remote_file_path, local_file_path)

            # Upload the output file to Clowder2
            file_id = pyclowder.files.upload_to_dataset(connector, host, secret_key, dataset_id, local_file_path,
                                                      check_duplicate=False)

        finally:
            # Close the SSH connection
            sftp.close()
            ssh_client.close()


if __name__ == "__main__":
    extractor = SubmitJob()
    extractor.start()
