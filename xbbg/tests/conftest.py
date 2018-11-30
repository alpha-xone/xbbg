def pytest_addoption(parser):

    parser.addoption(
        '--with_bbg', action='store_true', default=False,
        help='Run tests with Bloomberg connections'
    )
