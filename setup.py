from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'description.rst').read_text(encoding='utf-8')

setup(
    name='pyclowder',
    version='3.0.5',
    description='Python SDK for the Clowder Data Management System',
    long_description=long_description,

    author='Rob Kooper',
    author_email='kooper@illinois.edu',

    url='https://clowderframework.org',

    license='BSD',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate you support Python 3. These classifiers are *not*
        # checked by 'pip install'. See instead 'python_requires' below.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        "Programming Language :: Python :: 3.10",
    ],
    keywords=['clowder', 'data management system'],

    packages=find_packages(),

    python_requires='>=3.6, <4',

    install_requires=[
        'pika',
        'PyYAML',
        'requests',
        'requests-toolbelt',
    ],

    extras_require={  # Optional
        'dev': ['check-manifest'],
        'test': ['coverage'],
    },

    entry_points={  # Optional
        'console_scripts': [
            'sample=sample:main',
        ],
    },

    project_urls={  # Optional
        'Bug Reports': 'https://github.com/clowder-framework/pyclowder/issues',
        'Source': 'https://github.com/clowder-framework/pyclowder',
    },
)
