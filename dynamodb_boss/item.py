from uuid import uuid4

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError


DELETE_FAILED = 'delete failed'
REVTAG_COLLISION = 'revtag collision'
SAVE_FAILED = 'save failed'


class DynamoDBItemException(Exception):
    pass


class DynamoDBItem(object):

    PARTITION_KEY_NAME = ''
    SORT_KEY_NAME = ''
    TABLE_NAME = ''

    def __init__(self, boss, **kwargs):
        self.__dict__.update(**kwargs)
        # all non-dynamodb attributes should start with an underscore
        self._boss = boss
        self._table = boss.GetTable(self)

    def Delete(self):
        resp = self._table.delete_item(Key=self.key)
        if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
            print('Delete failed: %s' % resp['ResponseMetadata'])
            raise DynamoDBItemException(DELETE_FAILED)
        return resp

    @property
    def item_dict(self):
        item_dict = dict(self.__dict__)
        # remove all non-dynamodb attributes (those that start with an
        #   underscore) from the item_dict; iterate a list() of the dict keys
        #   because we are going to delete some of the keys from the dict
        for k in list(item_dict.keys()):
            if k[0] == '_':
                del item_dict[k]
        # hook for child classes to remove any other desired attributes
        self._PruneNonPersistentAttributes(item_dict)
        return item_dict

    @property
    def key(self):
        key = {
            self.PARTITION_KEY_NAME: getattr(self, self.PARTITION_KEY_NAME)
            }
        if self.SORT_KEY_NAME:
            key[self.SORT_KEY_NAME] = getattr(self, self.SORT_KEY_NAME)
        return key

    def _PruneNonPersistentAttributes(self, item):
        """
            Override this method for child classes that store non-persistent
                or non-persistable attributes in the instance and need to
                remove them from the item dict before storing the item in
                DynamoDB.
        """
        pass

    def Save(self):
        old_revtag = getattr(self, 'revtag', None)
        self.revtag = str(uuid4())
        kwargs = {'Item': self.item_dict}
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


