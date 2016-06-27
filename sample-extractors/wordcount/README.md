A simple extractor that counts the number of characters, words and lines in a text file.

# Docker

This extractor is ready to be run as a docker container. To build the docker container run:

```
docker build -t clowder_wordcount .
```

To run the docker containers use:

```
docker run -t -i --rm -e "RABBITMQ_URI=amqp://rabbitmqserver/clowder" clowder_wordcount
docker run -t -i --rm --link clowder_rabbitmq_1:rabbitmq clowder_wordcount
```

The RABBITMQ_URI and RABBITMQ_EXCHANGE environment variables can be used to control what RabbitMQ server and exchange it will bind itself to, you can also use the --link option to link the extractor to a RabbitMQ container.

# Commandline Execution

To execute the extractor from the command line you will need to have the required packages installed. It is highly recommended to use python virtual environment for this. You will need to create a virtual environment first, then activate it and finally install all required packages.

```
virtualenv /home/clowder/virtualenv/wordcount
. /home/clowder/virtualenv/wordcount/bin/activate
pip install -r /home/clowder/extractors/wordcount/requirements.txt
```

To start the extractor you will need to load the virtual environment and start the extractor.

```
. /home/clowder/virtualenv/wordcount/bin/activate
/home/clowder/extractors/wordcount/wordcount.py
```

# Systemd Start

The example service file provided in sample-extractors will start the docker container at system startup. This can be used with CoreOS or RedHat systems to make sure the wordcount extractor starts when the machine comes online. This expects the docker system to be installed.

All you need to do is copy clowder-wordcount.service to /etc/systemd/system and run, edit it to set the parameters for rabbitmq and run the following commands:

```
systemctl enable clowder-wordcount.service
systemctl start clowder-wordcount.service
```

To see the log you can use:

```
journalctl -f -u clowder-wordcount.service
```

# Upstart

The example conf file provided in sample-extractors will start the extractor on an Ubuntu system. This assumes that the system is setup for commandline execution. This will make it so the wordcount extractor starts when the system boots up. This extractor can be configured by specifying the same environment variables as using in the docker container. Any of the console output will go into /var/log/upstart/wordcount.log.
