import yaml


class Config:
    def __init__(self):
        self.log_file = ''
        self.log_level = ''
        self.listen_host = ''
        self.listen_port = ''
        self.clickhouse_scheme = ''
        self.clickhouse_host = ''
        self.clickhouse_port = ''
        self.encoding = 'utf-8'
        with open('config.yml') as fp:
            self.__dict__.update(yaml.safe_load(fp))


config: Config = Config()
