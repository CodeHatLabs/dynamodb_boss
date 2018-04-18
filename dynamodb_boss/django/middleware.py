from django.utils.deprecation import MiddlewareMixin
from django.utils.functional import SimpleLazyObject

from dynamodb_boss.boss import dynamodb_boss_pool


class DynamoDBBossMiddleware(MiddlewareMixin):

    def process_request(self, request):
        request.boss = SimpleLazyObject(lambda: dynamodb_boss_pool.Get())


