"""
Setup script for task_buffet.
"""

import os
import setuptools


def read(fname):
    """Construct the name and descriptions from README.md."""
    text = open(os.path.join(os.path.dirname(__file__), fname)).read()
    text = text.split('\n\n')
    name = text[0].lstrip('#').strip()
    description = text[1].strip('.')
    long_description = text[2]
    return name, description, long_description


def main():
    """Run the setup."""
    _, DESCRIPTION, LONG_DESCRIPTION = read('README.md')
    NAME = 'task_buffet'
    setuptools.setup(
        name=NAME,
        version='0.1',
        author='Julien-Charles Levesque',
        author_email='levesque.jc@gmail.com',
        url='http://github.com/jclevesque/' + NAME,
        description=DESCRIPTION,
        long_description=LONG_DESCRIPTION,
        license='MIT',
        packages=setuptools.find_packages(),
        install_requires=['numpy'],
        entry_points={'console_scripts':
            ['task-buffet = task_buffet.cli:main']}
        )


if __name__ == '__main__':
    main()
