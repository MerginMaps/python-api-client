# py-client

To generate py client from swagger definition run following:
```
docker run --rm -v ${PWD}:/local swaggerapi/swagger-codegen-cli generate -i /local/swagger.yaml -l python -o /local/client/ -c /local/swagger_config_client.json
```
