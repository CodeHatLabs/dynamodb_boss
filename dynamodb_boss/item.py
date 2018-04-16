from uuid import uuid4

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError


DELETE_FAILED = 'delete failed'
REVTAG_COLLISION = 'revtag collision'
SAVE_FAILED = 'save failed'


class DynamoDBItemException(Exception):
    pass


def item_factory(boss, item_class, dynamodb_item_dict):
    class Morph(object):
        def __init__(self, boss, item_class, dynamodb_item_dict):
            self.__class__ = item_class
            self.__dict__.update(dynamodb_item_dict)
            self._table = boss.GetTable(self)
    return Morph(boss, item_class, dynamodb_item_dict)


class DynamoDBItem(object):

    PARTITION_KEY_NAME = ''
    SORT_KEY_NAME = ''
    TABLE_NAME = ''

    def __init__(self, boss, **kwargs):
        self.__dict__.update(**kwargs)
        self._boss = boss
        self._table = boss.GetTable(self)

    def Delete(self):
        kwargs = {'Key': self.GetKey()}
        resp = self._table.delete_item(**kwargs)
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            print('Delete failed: %s' % resp['ResponseMetadata'])
            raise DynamoDBItemException(DELETE_FAILED)
        return resp

    def GetItem(self):
        item_dict = dict(self.__dict__)
        del item_dict['_table']
        self._GetItemPruneNonPersistentAttributes(item_dict)
        return item_dict

    def _GetItemPruneNonPersistentAttributes(self, item):
        """
            Override this method for child classes that store non-persistent
                or non-persistable attributes in the instance and need to
                remove them from the item dict before storing the item in
                DynamoDB.
        """
        pass

    def GetKey(self):
        result = {
            self.PARTITION_KEY_NAME: getattr(self, self.PARTITION_KEY_NAME)
            }
        if self.SORT_KEY_NAME:
            result[self.SORT_KEY_NAME] = getattr(self, self.SORT_KEY_NAME)
        return result

    def Save(self):
        old_revtag = getattr(self, 'revtag', None)
        self.revtag = str(uuid4())
        kwargs = {'Item': self.GetItem()}
        if old_revtag:
            kwargs['ConditionExpression'] = Attr('revtag').eq(old_revtag)
        try:
            resp = self._table.put_item(**kwargs)
            if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                print('Save failed: %s' % resp['ResponseMetadata'])
                raise DynamoDBItemException(SAVE_FAILED)
            return resp
        except ClientError as ex:
            if ex.response['Error']['Code'] \
                                    == 'ConditionalCheckFailedException':
                print('Oops! Someone else already edited this item: %s' % kwargs['Item'])
                raise DynamoDBItemException(REVTAG_COLLISION)
            raise


