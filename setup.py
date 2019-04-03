from setuptools import setup, find_packages


def description():
    """Return description in Restructure Text format."""

    with open('description.rst') as f:
        return f.read()


setup(name='pyclowder',
      version='2.2.0',
      packages=find_packages(),
      description='Python SDK for the Clowder Data Management System',
      long_description=description(),
      author='Rob Kooper',
      author_email='kooper@illinois.edu',

      url='https://clowder.ncsa.illinois.edu',
      project_urls={
        'Source': 'https://opensource.ncsa.illinois.edu/bitbucket/scm/cats/pyclowder2.git',
      },

      license='BSD',
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7'
      ],
      keywords=['clowder', 'data management system'],

      install_requires=[
          'enum34==1.1.6',
          'pika==1.0.0',
          'PyYAML==5.1',
          'requests==2.21.0',
      ],

      include_package_data=True,
      zip_safe=True,
      )
