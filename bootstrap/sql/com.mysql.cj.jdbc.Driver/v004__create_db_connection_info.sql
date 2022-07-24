UPDATE team_entity
SET json = JSON_INSERT(json, '$.teamType', 'Department');

ALTER TABLE team_entity
ADD teamType VARCHAR(64) GENERATED ALWAYS AS (json ->> '$.teamType') NOT NULL;

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.database')
WHERE serviceType = 'DynamoDB';

UPDATE dbservice_entity
SET json = JSON_REMOVE(json, '$.connection.config.connectionOptions')
WHERE serviceType = 'DeltaLake';

UPDATE dashboard_service_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.connection.config.username'),
        '$.connection.config.clientId',
        JSON_EXTRACT(json, '$.connection.config.username')
    )
WHERE serviceType = 'Looker';

UPDATE dashboard_service_entity
SET json = JSON_INSERT(
        JSON_REMOVE(json, '$.connection.config.password'),
        '$.connection.config.clientSecret',
        JSON_EXTRACT(json, '$.connection.config.password')
    )
WHERE serviceType = 'Looker';

UPDATE dashboard_service_entity
SET json = JSON_REMOVE(json, '$.connection.config.env')
WHERE serviceType = 'Looker';


CREATE TABLE IF NOT EXISTS test_definition_entity (
    id VARCHAR(36) GENERATED ALWAYS AS (json ->> '$.id') STORED NOT NULL,
    name VARCHAR(256) GENERATED ALWAYS AS (json ->> '$.name') NOT NULL,
    json JSON NOT NULL,
    updatedAt BIGINT UNSIGNED GENERATED ALWAYS AS (json ->> '$.updatedAt') NOT NULL,
    updatedBy VARCHAR(256) GENERATED ALWAYS AS (json ->> '$.updatedBy') NOT NULL,
    deleted BOOLEAN GENERATED ALWAYS AS (json -> '$.deleted'),
    UNIQUE (name)
);