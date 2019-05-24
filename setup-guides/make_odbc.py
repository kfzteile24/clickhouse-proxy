#!/usr/bin/env python3

import configparser
import sh

CH_DRIVER = 'ClickHouse'
CH_DSN = 'clickhouse_frontend'
CH_USER = 'batman'
CH_PASSWORD = 'batcave_secret'
CH_HOST = 'localhost'
CH_PORT = '8123'
CH_DB = 'criminals'
CH_VARIANT = 'w'


def update_file(fname, who):
    config = configparser.ConfigParser()

    config['ODBC Data Sources'] = {}
    drivers = config['ODBC Data Sources']
    drivers[CH_DSN] = CH_DRIVER

    config[CH_DSN] = {
        'driver': f'/usr/local/opt/clickhouse-odbc/lib/libclickhouseodbc{CH_VARIANT}.dylib',
        'description': 'Connection to criminals ClickHouse DB',
        'url': f'http://{CH_HOST}:{CH_PORT}/?database={CH_DB}&user={CH_USER}&password={CH_PASSWORD}',
        'server': CH_HOST,
        'password': CH_PASSWORD,
        'port': CH_PORT,
        'database': CH_DB,
        'uid': CH_USER,
        'sslmode': 'no'
    }

    with open(fname, 'w') as configfile:
        config.write(configfile)

    sh.chown(f"{who}:staff", fname)
    sh.chmod("644", fname)


def update_driver():
    lines = []
    skip_next = False
    with open('/Users/user/Library/ODBC/odbcinst.ini', 'r') as fp:
        for line in fp:
            if skip_next:
                skip_next = False
                continue
            lines.append(line)
            if line.startswith('[ClickHouse]'):
                lines.append(f'Driver = /usr/local/opt/clickhouse-odbc/lib/libclickhouseodbc{CH_VARIANT}.dylib')
                skip_next = True

    with open('/Users/user/Library/ODBC/odbcinst.ini', 'w') as fp:
        fp.writelines(lines)


update_file('/Users/user/.odbc.ini', 'user')
update_file('/Users/user/Library/ODBC/odbc.ini', 'root')
update_driver()
