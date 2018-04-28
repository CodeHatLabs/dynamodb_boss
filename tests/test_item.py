import logging
from os import environ
import pytest
from random import randint
from uuid import uuid4

from pygwanda.helpers import *

from dynamodb_boss.boss import DynamoDBBossPool
from dynamodb_boss.helpers import EZQuery
from dynamodb_boss.item import DynamoDBItem, DynamoDBItemException


logging.basicConfig(level=logging.WARN)


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
                    whatever_characters=UNAMBIGUOUS_ASCII, **kwargs):
        super().__init__(boss, **kwargs)
        self.partition_key = str(uuid4())
        self.whatever = random_unambiguous_string(whatever_length,
                                            whatever_characters)


class ItemPkSort(ItemPk):

    SORT_KEY_NAME = 'sort_key'
    TABLE_NAME = 'pksort'

    def __init__(self, boss, sort_key=None, whatever_length=10,
                    whatever_characters=UNAMBIGUOUS_ASCII, **kwargs):
        super().__init__(boss, whatever_length, whatever_characters, **kwargs)
        self.sort_key = sort_key or str(uuid4())


class TestItem:

    def test_item(self):
        boss = pool.Get()
        try:
            # test table 'pk'
            pk1 = ItemPk(boss)
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
                # test creating new item with duplcate key
                pk2 = ItemPk(boss, 10, UNAMBIGUOUS_UPPER)
                pk2.partition_key = pk1.partition_key
                got_exception = False
                try:
                    pk2.Save()
                except DynamoDBItemException as ex:
                    got_exception = True
                assert got_exception
            finally:
                pk1.Delete()
            # leave one in the table to test the ttl
            pk3 = ItemPk(boss, ttl_seconds=600)
            pk3.Save()
                
            # test table 'pksort'
            pks1 = ItemPkSort(boss, 'AAAAA')
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
                # test creating new item with duplcate key
                pks2 = ItemPkSort(boss)
                pks2.partition_key = pks1.partition_key
                pks2.sort_key = pks1.sort_key
                got_exception = False
                try:
                    pks2.Save()
                except DynamoDBItemException as ex:
                    got_exception = True
                assert got_exception
            finally:
                pks1.Delete()
            pks1 = ItemPkSort(boss, 'XYZZY')
            pks1.Save()
            pks2 = ItemPkSort(boss, 'CBAAB', ttl_seconds=None)
            pks2.partition_key = pks1.partition_key
            pks2.Save()
            pks3 = ItemPkSort(boss, 'MNOON', ttl_seconds=0)
            pks3.partition_key = pks1.partition_key
            pks3.Save()
            assert hasattr(pks3, 'ttl_expires')
            try:
                items = EZQuery(pks1._table, 'partition_key', pks1.partition_key)['Items']
                assert len(items) == 3
                assert items[0]['sort_key'] < items[1]['sort_key']
                assert items[1]['sort_key'] < items[2]['sort_key']
                assert 'ttl_seconds' not in items[0]
                assert 'ttl_seconds' not in items[1]
                assert 'ttl_seconds' not in items[2]
                assert 'ttl_expires' not in items[0]
                assert 'ttl_expires' in items[1]
                assert 'ttl_expires' not in items[2]
                items = EZQuery(pks1._table, 'partition_key', pks1.partition_key, reverse=True)['Items']
                assert len(items) == 3
                assert items[0]['sort_key'] > items[1]['sort_key']
                assert items[1]['sort_key'] > items[2]['sort_key']
            finally:
                pks1.Delete()
                pks2.Delete()
                pks3.Delete()
            # leave one in the table to test the ttl
            pks4 = ItemPkSort(boss, 'HOWLONG', ttl_seconds=600)
            pks4.Save()
        finally:
            pool.Release(boss)


