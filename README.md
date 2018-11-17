# xbbg

Bloomberg data toolkit

## Installation:

### Bloomberg Binaries:

- Reference: https://www.bloomberg.com/professional/support/api-library/

#### Install with `pip`

```bash
python -m pip install --index-url=https://bloomberg.bintray.com/pip/simple blpapi
```

#### Install in `pipenv`

- Add `source` in `Pipfile`

```
[[source]]
name = "bbg"
url = "https://bloomberg.bintray.com/pip/simple"
verify_ssl = true
```

- Run `pipenv`

```commandline
cd ...\xbbg\env
pipenv install blpapi
```

### From pypi:

```commandline
pip install xbbg
```

### From github (recommended):

```commandline
pip install git+https://github.com/alpha-xone/xbbg.git -U
```
