import boto3

from pygwanda.resource_pool import ResourcePool


def item_factory(boss, item_class, dynamodb_item_dict):
    class Morph(object):
        def __init__(self, boss, item_class, dynamodb_item_dict):
            self.__class__ = item_class
            self.__dict__.update(dynamodb_item_dict)
            self._table = boss.GetTable(self)
    return Morph(boss, item_class, dynamodb_item_dict)


class DynamoDBBoss(object):

    def __init__(self, aws_access_key_id, aws_secret_access_key,
                                region_name, table_name_prefix=''):
        self.session = boto3.Session(
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            region_name = region_name
            )
        self.dynamodb = self.session.resource('dynamodb')
        self.table_name_prefix = table_name_prefix
        self.tables = {}

    def GetItem(self, table_class, pk, sort=None):
        tbl = self.GetTable(table_class)
        key = {table_class.PARTITION_KEY_NAME: pk}
        if table_class.SORT_KEY_NAME:
            key[table_class.SORT_KEY_NAME] = sort
        resp = tbl.get_item(Key=key)
        item = resp.get('Item')
        return item_factory(self, table_class, item) if item else None

    def GetTable(self, name_or_class):
        table_name = name_or_class \
            if type(name_or_class) == type('') \
            else name_or_class.TABLE_NAME
        if not table_name in self.tables:
            self.tables[table_name] = self.dynamodb.Table('%s-%s' % (
                                        self.table_name_prefix, table_name))
        return self.tables[table_name]


class DynamoDBBossPool(ResourcePool):

    def __init__(self, aws_access_key_id, aws_secret_access_key,
                                region_name, table_name_prefix=''):
        super().__init__()
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.table_name_prefix = table_name_prefix

    def _CreateNewResource(self):
        return DynamoDBBoss(
            self.aws_access_key_id,
            self.aws_secret_access_key,
            self.region_name,
            self.table_name_prefix
            )


