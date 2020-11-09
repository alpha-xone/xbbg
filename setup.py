import pathlib
from os import path
from setuptools import setup, find_packages

# for pip >= 10
try:
    from pip._internal.req import parse_requirements
# for pip <= 9.0.3
except ImportError:
    from pip.req import parse_requirements

PACKAGE_ROOT = pathlib.Path(__file__).parent


def parse_version(package):
    """
    Parse versions
    """
    init_file = f'{PACKAGE_ROOT}/{package}/__init__.py'
    with open(init_file, 'r', encoding='utf-8') as f:
        for line in f.readlines():
            if '__version__' in line:
                return line.split('=')[1].strip()[1:-1]
    return ''


def parse_markdown():
    """
    Parse markdown as description
    """
    readme_file = f'{PACKAGE_ROOT}/README.md'
    if path.exists(readme_file):
        with open(readme_file, 'r', encoding='utf-8') as f:
            long_description = f.read()
        return long_description


def parse_description(markdown=True):
    """
    Parse the description in the README file
    """
    if markdown: return parse_markdown()

    try:
        from pypandoc import convert

        readme_file = f'{PACKAGE_ROOT}/docs/index.rst'
        if not path.exists(readme_file):
            raise ImportError
        return convert(readme_file, 'rst')

    except ImportError:
        return parse_markdown()


if __name__ == '__main__':

    setup(
        name='xbbg',
        version=parse_version('xbbg'),
        description='Intuitive Bloomberg data API',
        long_description=parse_description(),
        long_description_content_type='text/markdown',
        url='https://github.com/alpha-xone/xbbg',
        author='Alpha x1',
        author_email='alpha.xone@outlook.com',
        license='Apache',
        classifiers=[
            "License :: OSI Approved :: Apache Software License",
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
        ],
        include_package_data=True,
        package_data={
            'yaml': ['xbbg/markets/*.yml']
        },
        install_requires=[
            str(getattr(ir, 'req' if hasattr(ir, 'req') else 'requirement'))
            for ir in parse_requirements(
                f'{PACKAGE_ROOT}/requirements.txt', session='hack'
            )
        ],
        packages=find_packages(include=['xbbg', 'xbbg.*']),
        dependency_links=[
            'https://bloomberg.bintray.com/pip/simple',
        ],
    )

    print('\nBloomberg API')
    print('^^^^^^^^^^^^^\n')
    print('pip install --index-url=https://bloomberg.bintray.com/pip/simple blpapi')
