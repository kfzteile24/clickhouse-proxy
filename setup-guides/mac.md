# Setting up ODBC on a Mac

Hit or miss, this is done in a long trial-and-error queue that eventually worked.

The ODBC driver is needed because it's a bridge between Tableau and the HTTP endpoint for ClickHouse that gets proxied through this software.

##### Software that's installed in the process, but that isn't known if it's needed:
* ODBC Manager
* iODBC Administrator


## Build Clickhouse ODBC Driver

Hoping that you do have `homebrew` installed, and any other dependencies that might be required but are not documented (follow the errors in that case)

```
brew install https://raw.githubusercontent.com/proller/homebrew-core/chodbc/Formula/clickhouse-odbc.rb
```

This will install the latest release of Clickhouse-ODBC. Hopefully it's not broken (fingers crossed). Version 20190523 works.

## Set up the ODBC DSN

It's very important to get this right, without any errors. That's why there's a script here, `make_odbc.py`. To run it you need to also install `sh` with `pip install sh`. You also need to overwrite the connection details. But it's badly written, so help it out.

#### The Driver
The driver has to be registered in `/Users/<user>/Library/ODBC/odbcinst.ini`
```
[ODBC Drivers]
.....
.....
ClickHouse = Installed

.....

[ClickHouse]
Driver = /usr/local/opt/clickhouse-odbc/lib/libclickhouseodbcw.dylib
```

#### The DSN
Not sure which file exactly, so both files will do: `/Users/<user>/.odbc.ini` and `/Users/<user>/Library/ODBC/odbc.ini`

```
[ODBC Data Sources]
batcave_data = ClickHouse

[batcave_data]
driver = /usr/local/opt/clickhouse-odbc/lib/libclickhouseodbcw.dylib
description = Connection to my own batcave database that helps me beat Superman
url = http://batcave:8123/?database=kryptonite_signals&user=Batman&password=nanananana
# or https
url = https://batcave:8433/?database=kryptonite_signals&user=Batman&password=nanananana
server = batcave
password = nanananana
port = 8123 # or 8433 for ssh
database = kryptonite_signals
uid = Batman
```

This is the normal DSN. If you want to direct it to this proxy, you just change the host or port. The rest should be the same.
