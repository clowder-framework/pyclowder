A simple test extractor that verifies the functions of file in pyclowder.

# Docker

This extractor is ready to be run as a docker container, the only dependency is a running Clowder instance. Simply build and run.

1. Start Clowder V2. For help starting Clowder V2, see our [getting started guide](https://github.com/clowder-framework/clowder2/blob/main/README.md).

2. First build the extractor Docker container:

```
# from this directory, run:

docker build -t test-file-extractor .
```

3. Finally run the extractor:

```
docker run -t -i --rm --net clowder_clowder -e "RABBITMQ_URI=amqp://guest:guest@rabbitmq:5672/%2f" --name "test-file-extractor" test-file-extractor
```

Then open the Clowder web app and run the wordcount extractor on a .txt file (or similar)! Done.

### Python and Docker details

You may use any version of Python 3. Simply edit the first line of the `Dockerfile`, by default it uses `FROM python:3.8`.

Docker flags:

- `--net` links the extractor to the Clowder Docker network (run `docker network ls` to identify your own.)
- `-e RABBITMQ_URI=` sets the environment variables can be used to control what RabbitMQ server and exchange it will bind itself to. Setting the `RABBITMQ_EXCHANGE` may also help.
  - You can also use `--link` to link the extractor to a RabbitMQ container.
- `--name` assigns the container a name visible in Docker Desktop.

## Troubleshooting

**If you run into _any_ trouble**, please reach out on our Clowder Slack in the [#pyclowder channel](https://clowder-software.slack.com/archives/CNC2UVBCP).

Alternate methods of running extractors are below.

# Commandline Execution

To execute the extractor from the command line you will need to have the required packages installed. It is highly recommended to use python virtual environment for this. You will need to create a virtual environment first, then activate it and finally install all required packages.

```
  Step 1 - Start clowder docker-compose 
  Step 2 - Starting heartbeat listener 
          virtualenv clowder2-python (try pipenv)
          source clowder2-python/bin/activate
  Step 3 - Run heatbeat_listener_sync.py to register new extractor (This step will likely not be needed in future)
            cd ~/Git/clowder2/backend
	       pip install email_validator
        copy heartbeat_listener_sync.py to /backend from /backend/app/rabbitmq
	    python heartbeat_listener_sync.py
	
  Step 4 - Installing pyclowder branch & running extractor
	    source ~/clowder2-python/bin/activate
	    pip uninstall pyclowder

	    # the pyclowder Git repo should have Todd's branch activated (50-clowder20-submit-file-to-extractor)
	    pip install -e ~/Git/pyclowder
	
	    cd ~/Git/pyclowder/sample-extractors/test-file-extractor
	    export CLOWDER_VERSION=2   
	    export CLOWDER_URL=http://localhost:8000/

	    python test-file-extractor.py

	
  Step 5 = # post a particular File ID (text file) to the new extractor
    POST http://localhost:3002/api/v2/files/639b31754241665a4fc3e513/extract?extractorName=ncsa.test-file-extractor
    
    Or,
    Go to Clowder UI and submit a file for extraction
```

# Run the extractor from Pycharm
  You can run the heartbeat_listener_sync.py and test_file_extractor.py from pycharm. 
  Create a pipenv (generally pycharm directs you to create one when you first open the file). To run test_file_extractor.py,
  add 'CLOWDER_VERSION=2' to environment variable in run configuration.
