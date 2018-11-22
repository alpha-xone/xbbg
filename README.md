# xbbg

[![Documentation Status](https://readthedocs.org/projects/xbbg/badge/?version=latest)](https://xbbg.readthedocs.io/en/latest/?badge=latest)

Bloomberg data toolkit for humans

# Installation:

## From pypi:

```
pip install xbbg
```

## From github:

```
pip install git+https://github.com/alpha-xone/xbbg.git -U
```

# Documentation

## Examples

- BDP

```python
from xbbg import blp

blp.bdp(tickers='NVDA US Equity', flds=['Security_Name', 'GICS_Sector_Name'])
```

Output:

```
           ticker             field                   value
0  NVDA US Equity     Security_Name             NVIDIA Corp
1  NVDA US Equity  GICS_Sector_Name  Information Technology
```

- BDH

```python
from xbbg import blp

blp.bdh(
    tickers='SPX Index', flds=['High', 'Low', 'Last_Price'], 
    start_date='2018-10-10', end_date='2018-10-20',
)
```

Output:

```
ticker      SPX Index                    
field            High       Low  Last_Price
date                                       
2018-10-10   2,874.02  2,784.86    2,785.68
2018-10-11   2,795.14  2,710.51    2,728.37
2018-10-12   2,775.77  2,729.44    2,767.13
2018-10-15   2,775.99  2,749.03    2,750.79
2018-10-16   2,813.46  2,766.91    2,809.92
2018-10-17   2,816.94  2,781.81    2,809.21
2018-10-18   2,806.04  2,755.18    2,768.78
2018-10-19   2,797.77  2,760.27    2,767.78
```

- BDH with Excel compatible inputs

```python
from xbbg import blp

blp.bdh(
    tickers='SHCOMP Index', flds=['High', 'Low', 'Last_Price'], 
    start_date='2018-09-26', end_date='2018-10-20',
    Per='W', Fill='P', Days='A',
)
```

Output:

```
ticker      SHCOMP Index                    
field               High       Low  Last_Price
date                                          
2018-09-28      2,827.34  2,771.16    2,821.35
2018-10-05      2,827.34  2,771.16    2,821.35
2018-10-12      2,771.94  2,536.66    2,606.91
2018-10-19      2,611.97  2,449.20    2,550.47
```

- BDS

```python
from xbbg import blp

blp.bds('AAPL US Equity', 'DVD_Hist_All', DVD_Start_Dt='20180101', DVD_End_Dt='20180531')
```

Output:

```
            ticker         field                name         value  position
0   AAPL US Equity  DVD_Hist_All       Declared Date    2018-05-01         0
1   AAPL US Equity  DVD_Hist_All             Ex-Date    2018-05-11         0
2   AAPL US Equity  DVD_Hist_All         Record Date    2018-05-14         0
3   AAPL US Equity  DVD_Hist_All        Payable Date    2018-05-17         0
4   AAPL US Equity  DVD_Hist_All     Dividend Amount          0.73         0
5   AAPL US Equity  DVD_Hist_All  Dividend Frequency       Quarter         0
6   AAPL US Equity  DVD_Hist_All       Dividend Type  Regular Cash         0
7   AAPL US Equity  DVD_Hist_All       Declared Date    2018-02-01         1
8   AAPL US Equity  DVD_Hist_All             Ex-Date    2018-02-09         1
9   AAPL US Equity  DVD_Hist_All         Record Date    2018-02-12         1
10  AAPL US Equity  DVD_Hist_All        Payable Date    2018-02-15         1
11  AAPL US Equity  DVD_Hist_All     Dividend Amount          0.63         1
12  AAPL US Equity  DVD_Hist_All  Dividend Frequency       Quarter         1
13  AAPL US Equity  DVD_Hist_All       Dividend Type  Regular Cash         1
```

- Intraday data

```python
from xbbg import blp

blp.bdib('BHP AU Equity', '2018-10-17').tail()
```

Output:

```
                           open  high   low  close   volume  numEvents
2018-10-17 15:56:00+11:00 33.62 33.65 33.62  33.64    16660        126
2018-10-17 15:57:00+11:00 33.65 33.65 33.63  33.64    13875        156
2018-10-17 15:58:00+11:00 33.64 33.65 33.62  33.63    16244        159
2018-10-17 15:59:00+11:00 33.63 33.63 33.61  33.62    16507        167
2018-10-17 16:10:00+11:00 33.66 33.66 33.66  33.66  1115523        216
```

- Intraday data within certain session

```python
from xbbg import blp

blp.intraday('7974 JT Equity', '2018-10-17', session='am_open_30').tail()
```

Output:

```
                            open   high    low  close volume numEvents
2018-10-17 09:27:00+09:00 39,970 40,020 39,970 39,990  10800        44
2018-10-17 09:28:00+09:00 39,990 40,020 39,980 39,980   6300        33
2018-10-17 09:29:00+09:00 39,970 40,000 39,960 39,970   3300        21
2018-10-17 09:30:00+09:00 39,960 40,010 39,950 40,000   3100        19
2018-10-17 09:31:00+09:00 39,990 40,000 39,980 39,990   2000        15
```

## Optimizations

This library uses a global Bloomberg connection on the backend.
Specically, `_xcon_` in `globals()` variable.
Each time when a Bloomberg function is called:

- If there's no live connections, a new connection will be initiated
    - But this new connection will be disconnected and deleted before exiting function
- If there's already a live connection, the existing connection will be used
    - This live connection will be kept untouched before exiting function

Since initiating connections takes time, if multiple queries are expected,
manually initiating a new connection is helpful before calling any queries:

```python
from xbbg.conn import create_connection

create_connection()
```

This will create Bloomberg connection on the backend to be shared by all queries followed.

## Storage

If `ROOT_DATA_PATH` is provided in `os.environ`, data can be saved locally.
Next time the same query will be loaded from local rather than Bloomberg.

Noted that local data usage must be compliant with Bloomberg Datafeed Addendum
(full description in `DAPI<GO>`):

> To access Bloomberg data via the API (and use that data in Microsoft Excel), 
> your company must sign the 'Datafeed Addendum' to the Bloomberg Agreement. 
> This legally binding contract describes the terms and conditions of your use 
> of the data and information available via the API (the “Data”). 
> The most fundamental requirement regarding your use of Data is that it cannot 
> leave the local PC you use to access the BLOOMBERG PROFESSIONAL service.

# Reference

## Main dependncy

- [pdbdp](https://github.com/matthewgilbert/pdblp) - pandas wrapper for Bloomberg Open API

## Bloomberg Binaries

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
