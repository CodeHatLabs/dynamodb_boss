import boto3

from pygwanda.resource_pool import ResourcePool

from . import conf


class DynamoDBBoss(object):

    def __init__(self, aws_access_key_id, aws_secret_access_key, region_name,
                                                        table_name_prefix=''):
        self.session = boto3.Session(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            region_name = region_name
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

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                                    region_name=None, table_name_prefix=''):
        self.aws_access_key_id = aws_access_key_id \
                        or conf.settings.DYNAMODB_BOSS_AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = aws_secret_access_key \
                        or conf.settings.DYNAMODB_BOSS_AWS_SECRET_ACCESS_KEY
        self.region_name = region_name or conf.settings.DYNAMODB_BOSS_REGION
        self.table_name_prefix = table_name_prefix \
                        or conf.settings.DYNAMODB_BOSS_TABLE_NAME_PREFIX

    def _CreateNewResource(self):
        return DynamoDBBoss(
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.region_name,
            self.table_name_prefix
            )


dynamodb_boss_pool = DynamoDBBossPool()


