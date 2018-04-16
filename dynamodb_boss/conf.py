import os


class Settings(object):

    DYNAMODB_BOSS_AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
    DYNAMODB_BOSS_AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
    DYNAMODB_BOSS_REGION = 'us-west-2'
    DYNAMODB_BOSS_TABLE_NAME_PREFIX = ''


settings = Settings()


