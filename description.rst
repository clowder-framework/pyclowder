This package provides standard functions for interacting with the Clowder
open source data management system. Clowder is designed to allow researchers
to build customized catalogs in the clouds to help you manage research data.

One of the most interesting aspects of Clowder is the ability to extract
metadata from any file. This ability is created using extractors. To make it
easy to create these extractors in python we have created a module called
clowder. Besides wrapping often used api calls in convenient python calls, we
have also added some code to make it easy to create new extractors.

Installation
------------

Install using pip (for most recent versions see: https://pypi.org/project/pyclowder/):

```
pip install pyclowder==2.7.0
```

Install pyClowder on your system by cloning this repo:

```
git clone https://github.com/clowder-framework/pyclowder.git
cd pyclowder
pip install -r requirements.txt
python setup.py install
```

or directly from GitHub:

```
pip install -r https://raw.githubusercontent.com/clowder-framework/pyclowder/master/requirements.txt git+https://github.com/clowder-framework/pyclowder.git
```

Quickstart example
------------------

See the [README](https://github.com/clowder-framework/pyclowder/tree/master/sample-extractors/wordcount#readme)
in `sample-extractors/wordcount`. Using Docker, no install is required.
