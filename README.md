# xbbg

Bloomberg data toolkit

# Installation:

## From pypi:

```
pip install xbbg
```

## From github (recommended):

```
pip install git+https://github.com/alpha-xone/xbbg.git -U
```

# Reference

## Bloomberg Binaries:

- Reference: https://www.bloomberg.com/professional/support/api-library/

### Install with `pip`

```
python -m pip install --index-url=https://bloomberg.bintray.com/pip/simple blpapi
```

### Install in `pipenv`

- Add `source` in `Pipfile`

```toml
[[source]]
name = "bbg"
url = "https://bloomberg.bintray.com/pip/simple"
verify_ssl = true
```

- Run `pipenv`

```
cd ...\xbbg\env
pipenv install blpapi
```
