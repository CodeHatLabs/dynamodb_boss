from os import environ
import pytest
from random import randint
from uuid import uuid4

from pygwanda.helpers import *

from dynamodb_boss.boss import DynamoDBBossPool
from dynamodb_boss.item import DynamoDBItem


pool = DynamoDBBossPool(
    aws_access_key_id = environ['DYNAMODB_BOSS_AWS_ACCESS_KEY_ID'],
    aws_secret_access_key = environ['DYNAMODB_BOSS_AWS_SECRET_ACCESS_KEY'],
    region_name = environ['DYNAMODB_BOSS_REGION'],
    table_name_prefix = environ['DYNAMODB_BOSS_TABLE_NAME_PREFIX']
    )


class ItemPk(DynamoDBItem):

    PARTITION_KEY_NAME = 'partition_key'
    TABLE_NAME = 'pk'

    def __init__(self, boss, whatever_length=10,
                    whatever_characters=UNAMBIGUOUS_ASCII):
        super().__init__(boss)
        self.partition_key = str(uuid4())
        self.whatever = random_unambiguous_string(whatever_length,
                                            whatever_characters)


class ItemPkSort(ItemPk):

    SORT_KEY_NAME = 'sort_key'
    TABLE_NAME = 'pksort'

    def __init__(self, boss, whatever_length=10,
                    whatever_characters=UNAMBIGUOUS_ASCII):
        super().__init__(boss, whatever_length, whatever_characters)
        self.sort_key = str(uuid4())


class TestItem:

    def test_item(self):
        boss = pool.Get()
        try:
            # test table 'pk'
            pk1 = ItemPk(boss, 10, UNAMBIGUOUS_UPPER)
            pk1.Save()
            try:
                _pk1 = boss.GetItem(ItemPk, pk1.partition_key)
                assert _pk1.partition_key == pk1.partition_key
                assert _pk1.revtag == pk1.revtag
                assert _pk1.whatever == pk1.whatever
                pk1.whatever = random_unambiguous_string(50, UNAMBIGUOUS_UPPER)
                pk1.Save()
                _pk2 = boss.GetItem(ItemPk, pk1.partition_key)
                assert _pk2.partition_key == pk1.partition_key
                assert _pk2.revtag == pk1.revtag
                assert _pk2.whatever == pk1.whatever
                assert _pk2.whatever != _pk1.whatever
                assert _pk2.revtag != _pk1.revtag
            finally:
                pk1.Delete()
            # test table 'pksort'
            pks1 = ItemPkSort(boss, 10, UNAMBIGUOUS_UPPER)
            pks1.Save()
            try:
                _pks1 = boss.GetItem(ItemPkSort, pks1.partition_key, pks1.sort_key)
                assert _pks1.partition_key == pks1.partition_key
                assert _pks1.sort_key == pks1.sort_key
                assert _pks1.revtag == pks1.revtag
                assert _pks1.whatever == pks1.whatever
                pks1.whatever = random_unambiguous_string(50, UNAMBIGUOUS_UPPER)
                pks1.Save()
                _pks2 = boss.GetItem(ItemPkSort, pks1.partition_key, pks1.sort_key)
                assert _pks2.partition_key == pks1.partition_key
                assert _pks2.sort_key == pks1.sort_key
                assert _pks2.revtag == pks1.revtag
                assert _pks2.whatever == pks1.whatever
                assert _pks2.whatever != _pks1.whatever
                assert _pks2.revtag != _pks1.revtag
            finally:
                pks1.Delete()
        finally:
            pool.Release(boss)


