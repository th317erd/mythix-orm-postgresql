# mythix-orm-postgresql

PostgreSQL database driver for [Mythix ORM](https://www.npmjs.com/package/mythix-orm).

## Install

```bash
npm i --save mythix-orm-postgresql
```

## Documentation

Documentation can be found at the [WIKI](https://github.com/th317erd/mythix-orm-postgresql/wiki).

Documentation for Mythix ORM can be found at the [Mythix ORM WIKI](https://github.com/th317erd/mythix-orm/wiki).

## Usage

```javascript
const { PostgreSQLConnection } = require('mythix-orm-postgresql');

(async function() {
  let connection = new PostgreSQLConnection({
    bindModels: true,
    models:     [ /* application models */ ],
    logger:     console,
    user:       'database-username',
    password:   'database-password',
    database:   'test-database',
    host:       '127.0.0.1',
    port:       5432,
  });

  await connection.start();

  // run application code

  await connection.stop();
})();
```

## Connection Options

| Option | Type | Default Value | Description |
| ------ | ---- | ------------- | ----------- |
| `bindModels` | `boolean` | `true` | Bind the models provided to this connection (see the Mythix ORM [Connection Binding](https://github.com/th317erd/mythix-orm/wiki/ConnectionBinding) article for more information). |
| `connectionTimeout` | `number` | `60000` | Number of milliseconds to wait for connection. |
| `host` | `string` | `undefined` | The domain/host used to connect to the database. |
| `idleTransactionTimeout` | `number` | `undefined` | Number of milliseconds before terminating any session with an open idle transaction. |
| `logger` | Logger Interface | `undefined` | Assign a logger to the connection. If a logger is assigned, then every query (and every error) will be logged using this logger. |
| `maxPoolConnections` | `number` | `10` | Maximum number of clients the connection pool should contain. |
| `models` | `Array<Model>` | `undefined` | Models to register with the connection (these models will be bound to the connection if the `boundModels` option is `true`).
| `password` | `string` | `undefined` | The password used to connect to the database. |
| `port` | `string` | `5432` | The port used to connect to the database. |
| `queryGenerator` | [QueryGenerator](https://github.com/th317erd/mythix-orm/wiki/QueryGeneratorBase) | <see>PostgreSQLQueryGenerator</see> | Provide an alternate `QueryGenerator` interface for generating SQL statements for PostgreSQL. This is not usually needed, as the `SQLiteConnection` itself will provide its own generator interface. However, if you want to customize the default query generator, or want to provide your own, you can do so using this option. |
| `queryTimeout` | `number` | `15000` | Number of milliseconds before a query call will timeout. |
| `statementTimeout` | `number` | `15000` | Number of milliseconds before a statement in query will time out. |
| `user` | `string` | `undefined` | The username used to connect to the database. |