# Introduction

Clowder is an open-source research data management system that supports curation of long-tail data and metadata across
multiple research domains and diverse data types. It uses a metadata extraction bus to perform data curation. Extractors
are software programs that do the extraction of specific metadata from a file or dataset (a group of related files).
The Simple Extractor Wrapper is a piece of software being developed to make the process of developing an extractor
easier. This document will provide the details of writing an extractor program using the Simple Extractor Wrapper.

# Goals of Simple Extractor Wrapper

An extractor can be written in any programming language as long as it can communicate with Clowder using a simple HTTP
web service API and RabbitMQ. It can be hard to develop an extractor from the scratch when you also consider the code
that is needed for this communication. To reduce this effort and to avoid code duplication, we created libraries written
in Python (PyClowder) and Java (JClowder) to make the processing of writing extractors easy in these languages. We chose
these languages since they are among the most popular ones and they continue to remain to so. Though this is the case,
there is still some overhead in terms of developing an extractor using these libraries. In order to make the process of
writing extractors even easier, we created a Simple Extractor Wrapper, that wraps around your existing source code and
converts your code into an extractor. As the name says, the extractor itself needs to be simple in nature. The extractor
will process a file and generate metadata in JSON format and/or create a file preview. Any other Clowder API endpoints
are not currently available through the Simple Extractor and the developer would have to fall back to using PyClowder,
JClowder or writing the extractor from scratch.

# Creating an Extractor

The main function of your program needs to accept the string format file path of the input file. It also needs to
return an object containing either metadata information ("metadata"), details about file previews ("previews") or both
in the following format:

```json
{
    "metadata": {},
    "previews": []
}
```

The metadata sub document will contain the metadata that is directly uploaded back to clowder and will be associated
with the file. The previews array is a list of filenames that are previews that will be uploaded to clowder and
associated with file. Once the previews are uploaded they will be removed from the drive.

When writing the code for the extractor you don't have to worry about interaction with clowder and any subpieces, you
can test your code locally in your development environment by calling the function that will process the file and see
if the result matches the output described above.

# Using Extractor in Clowder

Once you are done with the extractor and you have tested your code you can wrap the extractor in a docker image and test
this image in the full clowder environment. To do this you will need to create a Dockerfile as well as an
extractor_info.json file as well as some optional additional files need by the docker build process. Once you have these
files you can build you image using `docker build -t extractor-example .`. This will build the docker image and tag it
with the name extractor-example (you should replace this with a better name).

The dockerfile has 2 environment variables that need to be set:
- R_SCRIPT : the path on disk to the actual file that needs to be sourced for the function. This can be left blank if
  no file needs to be sourced (for example in case when the file is installed as a package).
- R_FUNCTION : the name of the function that needs to be called that takes a file as input and returns an object that
  contains the data described above.
There can be 2 additional files that are used when creating the docker image:
- packages.apt : a list of ubuntu packages that need to be installed for the default ubuntu repositories.
- docker.R : an R script that is run during the docker build process. This can be used to install any required R
  packages. Another option is to install the code if it is provided as an R package.

There also has to be an extractor_info.json file which contains information about the extractor and is used to by the
extractor framework to initialize the extractor as well as upload information to clowder about the extractor.

```json
{
   "@context": "<context root URL>",
   "name": "<extractor name>",
   "version": "<version number>",
   "description": "<extractor description>",
   "author": "<first name> <last name> <<email address>>",
   "contributors": [
       "<first name> <last name> <<email address>>",
       "<first name> <last name> <<email address>>",
     ],
   "contexts": [
    {
       "<metadata term 1>": "<URL definition of metadata term 1>",
        "<metadata term 2>": "<URL definition of metadata term 2>",
     }
   ],
   "repository": [
      {
    "repType": "git",
         "repUrl": "<source code URL>"
      }
   ],
   "process": {
     "file": [
       "<MIME type/subtype>",
       "<MIME type/subtype>"
     ]
   },
   "external_services": [],
   "dependencies": [],
   "bibtex": []
 }
```

Once the image with the extractor is build you can test this extractor in the clowder environment. To do this you will
need to start clowder first. This can be done using a single [docker-compose file](https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/pyclowder2/raw/docker-compose.yml).
You can start the full clowder stack using `docker-compose up -p clowder` in the same folder where you downloaded the 
docker-compose file. After some time you will have an instance of clowder running that you can access using:
http://localhost:9000/ (if you use docker with virtualbox the url will probably be http://192.168.99.100:9000/).

If this is the first time you have started clowder you will need to create an account. You will be asked to enter an
email address (use admin@example.com). If you look at the console where you started clowder using docker-compose you
will some text and a url of the form  http://localhost:9000/signup/57d93076-7eca-418e-be7e-4a06c06f3259. If you follow
this URL you will be able to create an account for clowder. If you used the admin@example.com email address this will
have admin privileges.

Once you have the full clowder stack running, you can start your extractor using 
`docker run --rm -ti --network clowder_clowder extractor-example`. This will start the extractor and show the output
of the extractor on the command line. Once the extractor has started successfully, you can upload the appropriate file
and it should show that it is being processed by the extractor. At this point you have successfully created an
extractor and deployed it in clowder.


