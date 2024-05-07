# Cassandra notes

## Use SigV4AuthProvider for authentication provider to AWS Keyspaces

```java
...
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
...

@Configuration
@EnableCassandraRepositories(basePackages = {"..."})
public class CassandraConfiguration extends AbstractCassandraConfiguration {

    ...
    @Override
    protected SessionBuilderConfigurer getSessionBuilderConfigurer() {
        AwsCredentials credentials = AwsBasicCredentials.create(keyspaceAccessKeyId, keyspaceSecretAccessKey)
        StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);
        SigV4AuthProvider authProvider = new SigV4AuthProvider(credentialsProvider, keyspaceRegion);
        return (builder) -> builder.withAuthProvider(authProvider);
    }
    ....
```

## Use AWS STS client with control and linked roles for cretion of the SigV4AuthProvider

```java
...
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
...

@Configuration
@EnableCassandraRepositories(basePackages = {"..."})
public class CassandraConfiguration extends AbstractCassandraConfiguration {
...
    private static final String keyspaceControlSessionName = "control-session";
    private static final String keyspaceLinkedSessionName = "linked-session";
...
    @Override
    protected SessionBuilderConfigurer getSessionBuilderConfigurer() {
        StsClient controlStsClient = StsClient.builder().region(Region.of(keyspaceRegion)).build();
        AssumeRoleRequest controlRole = AssumeRoleRequest.builder()
                .roleArn(keyspaceControlArn)
                .roleSessionName(keyspaceControlSessionName)
                .build();
        StsAssumeRoleCredentialsProvider controlAccount = StsAssumeRoleCredentialsProvider.builder()
                .stsClient(controlStsClient)
                .refreshRequest(controlRole).build();

        StsClient linkedStsClient = StsClient.builder()
                .region(Region.of(keyspaceRegion))
                .credentialsProvider(controlAccount)
                .build();
        AssumeRoleRequest linkedRole = AssumeRoleRequest.builder()
                .roleArn(keyspaceLinkedArn)
                .roleSessionName(keyspaceLinkedSessionName)
                .build();
        StsAssumeRoleCredentialsProvider linkedAccount = StsAssumeRoleCredentialsProvider.builder()
                .stsClient(linkedStsClient)
                .refreshRequest(linkedRole)
                .build();

        SigV4AuthProvider authProvider = new SigV4AuthProvider(linkedAccount, keyspaceRegion);
        return (builder) -> builder.withAuthProvider(authProvider);
    }
```

## Use Cassandra driver (CQL sesssion) and Spring Data for asynchronous inserting of entities.

> **NOTE:** With Spring Data for Apache Cassandra 2.0, the API supports asynch operations See: [AsyncCassandraOperations](https://github.com/spring-projects/spring-data-cassandra/blob/5923cbebbcc4688372fcf72d6d5464027cc3f0c9/spring-data-cassandra/src/main/java/org/springframework/data/cassandra/core/AsyncCassandraOperations.java) and [AsyncCassandraTemplate](https://github.com/spring-projects/spring-data-cassandra/blob/5923cbebbcc4688372fcf72d6d5464027cc3f0c9/spring-data-cassandra/src/main/java/org/springframework/data/cassandra/core/AsyncCassandraTemplate.java)

```java
    public <T> CompletableFuture<T> insertEntities(Class<T> entityClass, List<T> entities, InsertOptions options) {
        CassandraPersistentEntity<T> persistentEntity =
                cassandraOperations.getConverter().getMappingContext().getPersistentEntity(entityClass);
        PreparedStatement preparedStatement = prepareInsert(persistentEntity, options);
        
        List<CompletableFuture<...>> insertTasks = 
            entities.stream()
                .map(e -> insertEntity(e, persistentEntity, preparedStatement))
                .collect(Collectors.toList());

        ....
    }



    private <T> PreparedStatement prepareInsert(
            CassandraPersistentEntity<T> persistentEntity, 
            InsertOptions options) {
    
        CqlIdentifier tableName = persistentEntity.getTableName();
        InsertInto insertInto = QueryBuilder.insertInto(tableName);
        //add terms
        Map<CqlIdentifier, Term> values = buildTerms(persistentEntity);
        RegularInsert insert = insertInto.valuesByIds(values);
        //add options
        SimpleStatement statement = QueryOptionsUtil.addWriteOptions(insert, options).build();
        statement =  QueryOptionsUtil.addQueryOptions(statement, options);
        log.debug("Prepared for entity {} INSERT Query: {}", persistentEntity.getName(), statement.getQuery());
        //synchronously prepare statement for INSERT
        return cqlSession.prepare(statement);
    }

    private <T> Map<CqlIdentifier, Term> buildTerms(CassandraPersistentEntity<Y> entity) {
        Map<CqlIdentifier, Term> values = new LinkedHashMap<>();
        for (CassandraPersistentProperty property : entity) {
            if (property.isCompositePrimaryKey()) {
                CassandraPersistentEntity<T> compositePrimaryKey =
                        cassandraOperations.getConverter().getMappingContext().getRequiredPersistentEntity(property);
                values.putAll(buildTerms(compositePrimaryKey));

                continue;
            }

            values.put(property.getColumnName(), bindMarker());
        }

        return values;
    }

    private <T> CompletableFuture<String> insertEntity(
            T entity,
            CassandraPersistentEntity<T> persistentEntity,
            PreparedStatement preparedStatement) {

        Map<CqlIdentifier, Object> sink = new LinkedHashMap<>();
        cassandraOperations.getConverter().write(entity, sink, persistentEntity);
        Object[] boundItems = sink.values().toArray();
        BoundStatement boundStatement = preparedStatement.bind(boundItems);

        return cqlSession
                .executeAsync(boundStatement)
                .thenApply(this::handleInsert)
                .toCompletableFuture();
    }

    private String handleInsert(AsyncResultSet result) {
        if (!result.wasApplied()) {
            String message =
                    String.format("Failed to insert Event Entity with execution info [%s]",
                                  getExecutionFailureDetail(result.getExecutionInfo()));
            log.error(message);
            throw new EventsCassandraException(message);
        }

        return RESULT_SUCCESS;
    }

    private String getExecutionFailureDetail(ExecutionInfo executionInfo) {
        return executionInfo.getErrors().stream()
                .map(Map.Entry::getValue)
                .map(Throwable::getMessage)
                .collect(Collectors.joining(","));
    }
```

