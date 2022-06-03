An example extractor that counts the number of characters, words and lines in a text file.

# Docker

This extractor is ready to be run as a docker container, the only dependency is a running Clowder instance. Simply build and run.

1. Start Clowder. For help starting Clowder, see our [getting started guide](https://github.com/clowder-framework/clowder/blob/develop/doc/src/sphinx/userguide/installing_clowder.rst).

2. Build the extractor Docker container:

```
# from this directory, run:

docker build -t kastanday/maple_cpu_kastan .
```

3. Finally run the extractor:

```
docker run -t -i --rm --net clowder_clowder -e "RABBITMQ_URI=amqp://guest:guest@rabbitmq:5672/%2f" --name "wordcount" clowder_wordcount
```

```
docker run -t -i --rm --net clowder_clowder -e "RABBITMQ_URI=amqp://guest:guest@rabbitmq:5672/%2f" --name "maple_cpu_demo"  --shm-size=1gb kastanday/maple_cpu_kastan
```

Add shm-size

Now you can open the Clowder web app and run the wordcount extractor on a `.txt` file (or similar). Done!

### Details

- `--name` assigns the container a name visible in Docker Desktop.
- `--net` links the extractor to the Clowder Docker network (run `docker network ls` to identify your own.)
- `-e RABBITMQ_URI=` sets the environment variables can be used to control what RabbitMQ server and exchange it will bind itself to. Setting the `RABBITMQ_EXCHANGE` may also help.
  - You can also use `--link` to link the extractor to a RabbitMQ container.
- `--rm` removes the filesystem after the container is shut down. No files local to the container will be saved.
- `-it` runs the container [_interactively_](https://stackoverflow.com/questions/48368411/what-is-docker-run-it-flag).
- `--shm-size=1gb` increases the size of memory swap (`/dev/shm`), improving stability and performance using Ray.

## Troubleshooting

**If you run into _any_ trouble**, please reach out on our Clowder Slack in the [#pyclowder channel](https://clowder-software.slack.com/archives/CNC2UVBCP).

Alternate methods of running extractors are below.

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
