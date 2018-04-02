This package provides standard functions for interacting with the
Clowder open source data management system. Clowder is designed
to allow researchers to build customized catalogs in the clouds
to help you manage research data.

Installation
------------

The easiest way install pyclowder is using pip and pulling from PyPI.
Use the following command to install::

    pip install pyclowder

Because this system is still under rapid development, you may want to
install by cloning the repo using the following commands::

    git clone https://opensource.ncsa.illinois.edu/bitbucket/scm/cats/pyclowder2.git
    cd pyclowder2
    pip install -r requirements.txt
    python setup.py install

Or you can install directly from NCSA's Bitbucket::

    pip install -r https://opensource.ncsa.illinois.edu/bitbucket/projects/CATS/repos/pyclowder2/raw/requirements.txt git+https://opensource.ncsa.illinois.edu/bitbucket/scm/cats/pyclowder2.git

