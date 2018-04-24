# dynamodb_boss
## Python tools for DynamoDB

## Testing

To run tests: 

    Create test tables:
        dynamodb-boss-test-pk
            Partition key name: partition_key
        dynamodb-boss-test-pksort
            Partition key name: partition_key
            Sort key name: sort_key

    You must have the following env variables set before testing:
        DYNAMODB_BOSS_AWS_ACCESS_KEY_ID
        DYNAMODB_BOSS_AWS_SECRET_ACCESS_KEY
        DYNAMODB_BOSS_AWS_REGION

    * With Docker Compose:
        `docker-compose up test`

    * With Docker:
        `docker build -t dynamodb_boss:latest . && docker run dynamodb_boss:latest`

    * With Python 3.6 and pipenv:
        `./run-test.sh`

