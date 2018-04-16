import boto3

from pygwanda.resource_pool import ResourcePool

from . import conf


class DynamoDBBoss(object):

    def __init__(self, table_name_prefix=''):
        self.session = boto3.Session(
            aws_access_key_id = conf.settings.DYNAMODB_BOSS_AWS_ACCESS_KEY_ID,
            aws_secret_access_key = \
                            conf.settings.DYNAMODB_BOSS_AWS_SECRET_ACCESS_KEY,
            region_name = conf.settings.DYNAMODB_BOSS_REGION
            )
        self.dynamodb = self.session.resource('dynamodb')
        self.table_name_prefix = table_name_prefix
        self.tables = {}

    def GetTable(self, name_or_class):
        table_name = name_or_class \
            if type(name_or_class) == type('') \
            else name_or_class.TABLE_NAME
        if not table_name in self.tables:
            self.tables[table_name] = self.dynamodb.Table('%s-%s' % (
                                        self.table_name_prefix, table_name))
        return self.tables[table_name]


class DynamoDBBossPool(ResourcePool):

    def _CreateNewResource(self):
        return DynamoDBBoss(conf.settings.DYNAMODB_BOSS_TABLE_NAME_PREFIX)


dynamodb_boss_pool = DynamoDBBossPool()


