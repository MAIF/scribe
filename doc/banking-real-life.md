# Postgres Kafka event sourcing

This example is based on [bank example](), we'll replace our InMemoryEventStore by a real Event store using Postgres and Kafka.

First we need to import `scribe-jooq` module. This module contains an implementation of scribe for Postgres using Jooq.

## SQL

First thing first : we need to set up database.

Database and user creation: 

```sql
CREATE DATABASE eventsourcing;
CREATE USER eventsourcing WITH PASSWORD 'eventsourcing';
GRANT ALL PRIVILEGES ON DATABASE "eventsourcing" to eventsourcing;
```

Schema creation:

```sql
CREATE TABLE IF NOT EXISTS ACCOUNTS (
    id varchar(100) PRIMARY KEY,
    balance money NOT NULL
);

CREATE TABLE IF NOT EXISTS bank_journal (
  id UUID primary key,
  entity_id varchar(100) not null,
  sequence_num bigint not null,
  event_type varchar(100) not null,
  version int not null,
  transaction_id varchar(100) not null,
  event jsonb not null,
  metadata jsonb,
  context jsonb,
  total_message_in_transaction int default 1,
  num_message_in_transaction int default 1,
  emission_date timestamp not null default now(),
  user_id varchar(100),
  system_id varchar(100),
  published boolean default false,
  UNIQUE (entity_id, sequence_num)
);

CREATE SEQUENCE if not exists bank_sequence_num;
```

Here is what we need in the database:

* An `ACCOUNTS` table to keep our accounts safe, we kept it simple here to match our model
* A `BANK_JOURNAL` table that will contain our events
* A `BANK_SEQUENCE_NUM` to generate sequence_num of our events

# Code

First of all let's swap `scribe-core` dependency with `scribe-jooq`. This new dependency provides everything we need to set up postgres / kafka connection.

```xml
<dependency>
    <groupId>fr.maif</groupId>
    <artifactId>scribe-jooq</artifactId>
    <version>...</version>
</dependency>
```

## Event serialization

Let's start with event reading and writing. We need to declare a serializer to read / write events to DB.

```java
public class BankEventFormat implements JacksonEventFormat<String, BankEvent> {
    @Override
    public Either<String, BankEvent> read(String type, Long version, JsonNode json) {
        return API.Match(Tuple.of(type, version)).option(
                Case(BankEvent.MoneyDepositedV1.pattern2(), () -> Json.fromJson(json, BankEvent.MoneyDeposited.class)),
                Case(BankEvent.MoneyWithdrawnV1.pattern2(), () -> Json.fromJson(json, BankEvent.MoneyWithdrawn.class)),
                Case(BankEvent.AccountClosedV1.pattern2(), () -> Json.fromJson(json, BankEvent.AccountClosed.class)),
                Case(BankEvent.AccountOpenedV1.pattern2(), () -> Json.fromJson(json, BankEvent.AccountOpened.class))
        )
                .toEither(() -> "Unknown event type " + type + "(v" + version + ")")
                .flatMap(jsResult -> jsResult.toEither().mapLeft(errs -> errs.mkString(",")));
    }

    @Override
    public JsonNode write(BankEvent event) {
        return Json.toJson(event, JsonWrite.auto());
    }
}
```

We implemented this using [functionnal-json](https://github.com/MAIF/functional-json) library, since it provides nice utilities to handle / aggregate deserialization errors.

To allow event serialization / deserialization we also need to add some Jackson annotations (`@JsonCreator` and `@JsonProperty`) to events' constructors.

```java
public abstract class BankEvent implements Event {
    public static Type<MoneyWithdrawn> MoneyWithdrawnV1 = Type.create(MoneyWithdrawn.class, 1L);
    public static Type<AccountOpened> AccountOpenedV1 = Type.create(AccountOpened.class, 1L);
    public static Type<MoneyDeposited> MoneyDepositedV1 = Type.create(MoneyDeposited.class, 1L);
    public static Type<AccountClosed> AccountClosedV1 = Type.create(AccountClosed.class, 1L);

    public final String accountId;

    public BankEvent(String accountId) {
        this.accountId = accountId;
    }

    @Override
    public String entityId() {
        return accountId;
    }

    public static class MoneyWithdrawn extends BankEvent {
        public final BigDecimal amount;
        @JsonCreator
        public MoneyWithdrawn(@JsonProperty("accountId")String account, @JsonProperty("amount")BigDecimal amount) {
            super(account);
            this.amount = amount;
        }

        @Override
        public Type<?> type() {
            return MoneyWithdrawnV1;
        }
    }

    public static class AccountOpened extends BankEvent {
        @JsonCreator
        public AccountOpened(@JsonProperty("accountId")String id) {
            super(id);
        }

        @Override
        public Type<?> type() {
            return AccountOpenedV1;
        }
    }

    public static class MoneyDeposited extends BankEvent {
        public final BigDecimal amount;
        @JsonCreator
        public MoneyDeposited(@JsonProperty("accountId")String id, @JsonProperty("amount")BigDecimal amount) {
            super(id);
            this.amount = amount;
        }

        @Override
        public Type<?> type() {
            return MoneyDepositedV1;
        }
    }

    public static class AccountClosed extends BankEvent {
        @JsonCreator
        public AccountClosed(@JsonProperty("accountId")String id) {
            super(id);
        }

        @Override
        public Type<?> type() {
            return AccountClosedV1;
        }
    }
}
```

## Database connection

Speaking of database, we also need to set up a database connection somewhere.

In the sample application, this is made in `Bank` class, in real world application, this could be made in some configuration class.

```Java
public class Bank {
    // ...
    private DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setServerName("localhost");
        dataSource.setPassword("eventsourcing");
        dataSource.setUser("eventsourcing");
        dataSource.setDatabaseName("eventsourcing");
        dataSource.setPortNumbers(5432);
        return dataSource;
    }
    // ...
}
```

We also need a `TableNames` instance to provide information about created table name and sequence.

```java
public class Bank {
    //...
    private TableNames tableNames() {
        return new TableNames("bank_journal", "bank_sequence_num");
    }
    //...
}
```

Since this implementation will use a real database, we need to change TransactionContext type from `Tuple0` to `Connection` in `CommandHandler`.

This transaction context allows sharing database context for command verification and events insertion.

```java
public class BankCommandHandler implements CommandHandler<String, Account, BankCommand, BankEvent, Tuple0, Connection> {
    //...
}
```

## Kafka connection

To handle the kafka part, we need two things:
* A `KafkaSettings` instance, that should contain kafka location and keystore / truststore information (if needed)
* A `ProducerSettings` instance that will be used to publish events in kafka

```java
public class Bank {
    //...
    private KafkaSettings settings() {
        return KafkaSettings.newBuilder("localhost:29092").build();
    }

    private ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings(
            KafkaSettings kafkaSettings,
            JacksonEventFormat<String, BankEvent> eventFormat) {
        return kafkaSettings.producerSettings(actorSystem, JsonSerializer.of(
                eventFormat,
                JacksonSimpleFormat.empty(),
                JacksonSimpleFormat.empty()
            )
        );
    }
    //...
}
```


## Event store

Almost there !
We now need to instantiate an `EventStore` that will replace our `InMemoryEventStore` : `PostgresEventStore`.

This eventStore will handle any database / kafka interaction.

```java
public class Bank {
    //...
    private EventStore<Connection, BankEvent, Tuple0, Tuple0> eventStore(
            ActorSystem actorSystem,
            ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings, String topic,
            DataSource dataSource,
            ExecutorService executorService,
            TableNames tableNames,
            JacksonEventFormat<String, BankEvent> jacksonEventFormat) {
        KafkaEventPublisher<BankEvent, Tuple0, Tuple0> kafkaEventPublisher = new KafkaEventPublisher<>(actorSystem, producerSettings, topic);
        return PostgresEventStore.create(actorSystem, kafkaEventPublisher, dataSource, executorService, tableNames, jacksonEventFormat);
    }
    //...
}
```

## Event processor

The last step is to swap our `EventProcessor` with `PostgresKafkaEventProcessor`.

```java
public class Bank {
    //...
    public Bank(ActorSystem actorSystem,
                BankCommandHandler commandHandler,
                BankEventHandler eventHandler
                ) throws SQLException {
        this.actorSystem = actorSystem;
        JacksonEventFormat<String, BankEvent> eventFormat = new BankEventFormat();
        ProducerSettings<String, EventEnvelope<BankEvent, Tuple0, Tuple0>> producerSettings = producerSettings(settings(), eventFormat);
        DataSource dataSource = dataSource();

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        this.eventProcessor = PostgresKafkaEventProcessor.create(
                actorSystem,
                eventStore(actorSystem, producerSettings, "bank", dataSource, executorService, new TableNames("bank_journal", "bank_sequence_num") ,eventFormat),
                new JdbcTransactionManager(dataSource(), executorService),
                commandHandler,
                eventHandler,
                List.empty());
    }
    //...
}
```

We needed to provide an ExecutorService that will handle every database interaction. The indicated needs to be adapted in each application.

## Usage

Usage remains the same as in [in memory example](./banking.md).

[Complete executable example.](../demo-postgres-kafka)