import sys


def pytest_addoption(parser):

    parser.addoption(
        '--with_bbg', action='store_true', default=False,
        help='Run tests with Bloomberg connections'
    )


def pytest_configure(config):

    print(config)
    sys.pytest_call = True


def pytest_unconfigure(config):

    print(config)
    if hasattr(sys, 'pytest_call'):
        del sys.pytest_call
