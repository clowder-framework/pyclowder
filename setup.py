from setuptools import setup, find_packages

def description():

    with open('description.rst') as f:
        return f.read()


setup(name='pyclowder',
      version='2.0.0',
      packages=find_packages(),
      description='TERRA-REF utility library',
      long_description=description(),
      author='Rob Kooper',
      author_email='kooper@illinois.edu',

      url='clowder.ncsa.illinois.edu',
      project_urls={
        'Source': 'https://opensource.ncsa.illinois.edu/bitbucket/scm/cats/pyclowder2.git',
      },

      license='BSD',
      classifiers=[
        'Development Status :: 5 - Production',
        'Intended Audience :: Developers',
        'Topic :: Data Science :: Data Management System',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7'
      ],
      keywords=['clowder', 'data management system'],

      install_requires=[
          'enum34',
          'pika',
          'PyYAML',
          'requests',
      ],

      include_package_data=True,
      zip_safe=True,
      )
