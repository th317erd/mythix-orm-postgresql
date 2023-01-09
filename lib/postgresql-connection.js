/* eslint-disable no-magic-numbers */
/* eslint-disable camelcase */

'use strict';

const Nife                      = require('nife');
const { DateTime }              = require('luxon');
const PG                        = require('pg');
const PGFormat                  = require('pg-format');
const { Literals, Errors }      = require('mythix-orm');
const { SQLConnectionBase }     = require('mythix-orm-sql-base');
const PostgreSQLQueryGenerator  = require('./postgresql-query-generator');

const DEFAULT_TIMEOUT_MS = 30000;

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

/// Mythix ORM connection driver for PostgreSQL.
///
/// This inherits from [SQLConnectionBase](https://github.com/th317erd/mythix-orm-sql-base/wiki)
/// and so gets most of its SQL functionality from its parent class.
///
/// Extends: [SQLConnectionBase](https://github.com/th317erd/mythix-orm-sql-base/wiki)
class PostgreSQLConnection extends SQLConnectionBase {
  static dialect = 'postgresql';

  static DefaultQueryGenerator = PostgreSQLQueryGenerator;

  /// Create a new `PostgreSQLConnection` instance.
  ///
  /// Arguments:
  ///   options?: object
  ///     Options to provide to the connection. All options are optional, though `models`
  ///     is required before the connection is used. If not provided here to the constructor,
  ///     the application models can always be provided at a later time using the
  ///     [Connection.registerModels](https://github.com/th317erd/mythix-orm/wiki/ConnectionBase#method-registerModels) method.
  ///     | Option | Type | Default Value | Description |
  ///     | ------ | ---- | ------------- | ----------- |
  ///     | `bindModels` | `boolean` | `true` | Bind the models provided to this connection (see the Mythix ORM [Connection Binding](https://github.com/th317erd/mythix-orm/wiki/ConnectionBinding) article for more information). |
  ///     | `connectionTimeout` | `number` | `60000` | Number of milliseconds to wait for connection. |
  ///     | `host` | `string` | `undefined` | The domain/host used to connect to the database. |
  ///     | `idleTransactionTimeout` | `number` | `undefined` | Number of milliseconds before terminating any session with an open idle transaction. |
  ///     | `logger` | Logger Interface | `undefined` | Assign a logger to the connection. If a logger is assigned, then every query (and every error) will be logged using this logger. |
  ///     | `maxPoolConnections` | `number` | `10` | Maximum number of clients the connection pool should contain. |
  ///     | `models` | `Array<Model>` | `undefined` | Models to register with the connection (these models will be bound to the connection if the `boundModels` option is `true`).
  ///     | `password` | `string` | `undefined` | The password used to connect to the database. |
  ///     | `port` | `string` | `5432` | The port used to connect to the database. |
  ///     | `queryGenerator` | [QueryGenerator](https://github.com/th317erd/mythix-orm/wiki/QueryGeneratorBase) | <see>PostgreSQLQueryGenerator</see> | Provide an alternate `QueryGenerator` interface for generating SQL statements for PostgreSQL. This is not usually needed, as the `PostgreSQLConnection` itself will provide its own generator interface. However, if you want to customize the default query generator, or want to provide your own, you can do so using this option. |
  ///     | `queryTimeout` | `number` | `15000` | Number of milliseconds before a query call will timeout. |
  ///     | `statementTimeout` | `number` | `15000` | Number of milliseconds before a statement in query will time out. |
  ///     | `user` | `string` | `undefined` | The username used to connect to the database. |
  constructor(_options) {
    super(_options);

    this.setQueryGenerator(new PostgreSQLQueryGenerator(this));

    Object.defineProperties(this, {
      'pool': {
        writable:     true,
        enumerable:   false,
        configurable: true,
        value:        null,
      },
    });
  }

  isStarted() {
    return !!this.pool;
  }

  async getPoolConnection(_retryCount) {
    let retryCount = _retryCount || 0;

    try {
      return await this.pool.connect();
    } catch (error) {
      let options           = this.getOptions();
      let connectMaxRetries = (options.connectMaxRetries == null) ? 5 : options.connectMaxRetries;
      let connectRetryDelay = (options.connectRetryDelay == null) ? 5000 : options.connectRetryDelay;

      if (error.code === 'ECONNREFUSED') {
        if (retryCount < connectMaxRetries) {
          await sleep(connectRetryDelay);
          return this.getPoolConnection(retryCount + 1);
        }

        throw new Errors.MythixORMConnectionTimedOutError(error);
      }

      throw error;
    }
  }

  async start() {
    let options = this.getOptions();

    if (options.foreignConstraints != null && options.logger)
      options.logger.warn('"foreignConstraints" option is not supported by the PostgreSQL driver.');

    let opts = Object.assign(
      {
        allowExitOnIdle: true,
      },
      options,
      {
        statement_timeout:                   options.statementTimeout || DEFAULT_TIMEOUT_MS,
        query_timeout:                       options.queryTimeout || DEFAULT_TIMEOUT_MS,
        connectionTimeoutMillis:             options.connectionTimeout || 60000,
        idle_in_transaction_session_timeout: options.idleTransactionTimeout,
        max:                                 options.maxPoolConnections,
      },
    );

    const PGScope = (opts.native === true && PG.native) ? PG.native : PG;

    let pool = this.pool = new PGScope.Pool(opts);

    pool.on('connect', (client) => {
      this.emit('connect', client);
    });

    pool.on('acquire', (client) => {
      this.emit('acquire', client);
    });

    pool.on('error', (error, client) => {
      this.emit('error', error, client);
    });

    pool.on('remove', (client) => {
      this.emit('disconnect', client);
    });

    // Ensure that we can connect to the DB
    let client = await this.getPoolConnection();
    if (client)
      await client.release();
  }

  async stop() {
    if (!this.pool)
      return;

    await this.pool.end();
    this.pool = null;

    await super.stop();
  }

  _escape(value) {
    return PGFormat.literal(value);
  }

  _escapeID(value) {
    return PGFormat.ident(value);
  }

  getDefaultFieldValue(type) {
    switch (type) {
      case 'AUTO_INCREMENT':
        // This will get discarded by createTable...
        // but we need the value later to select the
        // proper column data type
        return new Literals.Literal('AUTOINCREMENT', { noDefaultStatementOnCreateTable: true, remote: true });
      case 'DATETIME_NOW':
        return new Literals.Literal('(FLOOR(EXTRACT(EPOCH FROM NOW()) * 1000))', { escape: false, remote: true });
      case 'DATE_NOW':
        return new Literals.Literal('(FLOOR(EXTRACT(EPOCH FROM NOW()) * 1000))', { escape: false, remote: true });
      case 'DATETIME_NOW_LOCAL':
        return DateTime.now().toMillis();
      case 'DATE_NOW_LOCAL':
        return DateTime.now().startOf('day').toMillis();
      default:
        return type;
    }
  }

  // eslint-disable-next-line no-unused-vars
  async enableForeignKeyConstraints(enable) {
    throw new Error(`${this.constructor.name}::enableForeignKeyConstraints: This operation is not supported for this connection type.`);
  }

  async exec(...args) {
    return await this.query(...args);
  }

  /// A raw query interface for the PostgreSQL database.
  ///
  /// This method is used internally to execute all
  /// SQL statements generated against PostgreSQL. For
  /// `SELECT` or `RETURNING` SQL statements, this will
  /// return the results in a formatted object: `{ rows: [ ... ], columns: [ ... ] }`.
  ///
  /// Arguments:
  ///   sql: string
  ///     The fully formed SQL statement to execute.
  ///   options?: object
  ///     Options for the operation.
  ///     | Option | Type | Default Value | Description |
  ///     | ------ | ---- | ------------- | ----------- |
  ///     | `logger` | Logger Interface | `undefined` | If provided, then the query and any errors encountered will be logged |
  ///     | `logResponse` | `boolean` | `false` | If `true`, then the response from PostgreSQL will be logged |
  ///     | `parameters` | `Array` or `Object` | `undefined` | Parameters to bind for [pg](https://node-postgres.com/features/queries) |
  ///
  /// Return: any
  ///   A `{ rows: [ ... ], columns: [ ... ] }` object if the statement is a
  ///   `SELECT` statement, or a statement with a `RETURNING` clause. Otherwise
  ///   the raw result from PostgreSQL will be returned for the statement executed.
  async query(_sql, _options, _client) {
    let sql = _sql;
    if (!sql)
      return;

    if (Nife.instanceOf(sql, 'string'))
      sql = { text: sql, rowMode: 'array' };
    else if (Nife.instanceOf(sql, 'object'))
      sql = Object.assign({ text: sql, rowMode: 'array' }, sql);

    let options       = _options || {};
    let logger        = options.logger || (this.getOptions().logger);
    let hasOwnClient;
    let client;

    try {
      client = _client || this.inTransaction;
      if (!client) {
        client = await this.getPoolConnection();
        hasOwnClient = true;
      }

      if (logger && sql.text)
        logger.log('QUERY: ', sql.text);

      let result;

      if (Nife.isNotEmpty(options.parameters))
        result = await client.query(sql, options.parameters);
      else
        result = await client.query(sql);

      if (client && hasOwnClient)
        await client.release();

      if (logger && options.logResponse) {
        logger.log('QUERY RESULT: ', {
          columns: (result.fields) ? result.fields.map((field) => field.name) : [],
          result:  (result.rows) ? result.rows : result,
        });
      }

      return this.formatResultsResponse(sql, result);
    } catch (error) {
      if (client && hasOwnClient)
        await client.release();

      if (logger) {
        logger.error(error);
        logger.error('QUERY: ', sql);
      }

      error.query = sql;

      throw error;
    }
  }

  /// Format the response from PostgreSQL for a `SELECT`
  /// or `RETURNING` statement.
  ///
  /// Arguments:
  ///   sql: string
  ///     The SQL statement that was executed.
  ///   result: any
  ///     The response from PostgreSQL.
  ///
  /// Return: { rows: Array<any>, columns: Array<string> }
  ///   The formatted response, containing all the rows returned by the query, and all
  ///   projected columns. The columns are converted to names only, so as to keep the
  ///   query interface consistent across all database drivers Mythix ORM supports.
  formatResultsResponse(sql, result) {
    if (!result.rows || !result.fields)
      return result;

    return {
      rows:    result.rows,
      columns: result.fields.map((field) => field.name),
    };
  }

  /// This method will start a transaction, or, if a transaction
  /// is already active, will instead start a `SAVEPOINT`.
  ///
  /// If an error is thrown in the provided `callback`, then the
  /// transaction or `SAVEPOINT` will be automatically rolled back.
  /// If the `callback` provided returns successfully, then the
  /// transaction will be committed automatically for you.
  ///
  /// Arguments:
  ///   callback: (connection: PostgreSQLConnection) => any
  ///     The transaction connection is passed to this callback as soon as
  ///     the transaction or `SAVEPOINT` is started. This often isn't needed
  ///     if the global [AsyncLocalStorage](https://github.com/th317erd/mythix-orm/wiki/AsyncStore) context is supported and in-use
  ///     (the default). If the [AsyncLocalStorage](https://github.com/th317erd/mythix-orm/wiki/AsyncStore) context is not in-use,
  ///     then this `connection` **must** be passed all the way through all
  ///     database calls made inside this callback.
  ///   options?: object
  ///     Options for the transaction operation. This includes the options listed below, as well as any
  ///     options that can be passed to <see>PostgreSQLConnection.query</see>. The `lock` options object has
  ///     one PostgreSQL specific sub-option named `mode`, that can be one of `ACCESS EXCLUSIVE` (the default),
  ///     `EXCLUSIVE`, or `ACCESS SHARE`. To understand what these do, refer to the [PostgreSQL documentation](https://www.postgresql.org/docs/15/sql-begin.html).
  ///     | Option | Type | Default Value | Description |
  ///     | ------ | ---- | ------------- | ----------- |
  ///     | `connection` | `PostgreSQLConnection` | `undefined` | The connection to use for the operation. This is generally only needed if you are already inside a transaction, and need to supply the transaction connection to start a sub-transaction (a `SAVEPOINT`). |
  ///     | `lock | [Lock Mode](https://github.com/th317erd/mythix-orm/wiki/ConnectionBase#method-getLockMode) | `undefined` | Specify the lock mode for the transaction. See [ConnectionBase.getLockMode](https://github.com/th317erd/mythix-orm/wiki/ConnectionBase#method-getLockMode) for more information. |
  ///
  /// Return: any
  ///   Return the result of the provided `callback`.
  async transaction(callback, _options, _retryCount) {
    let options       = { ...(this.getOptions() || {}), ...(_options || {}) };
    let inheritedThis = Object.create(options.connection || this.getContextValue('connection', this));
    let lockMode      = inheritedThis.getLockMode(options.lock);
    let savePointName;
    let client;
    let lockStatement;

    if (lockMode && lockMode.lock) {
      let Model = inheritedThis.getModel(lockMode.modelName);
      if (!Model)
        throw new Error(`${inheritedThis.constructor.name}::transaction: Request to lock table defined by model "${lockMode.modelName}", but model with that name can not be found.`);

      let escapedTableName  = inheritedThis.escapeID(Model.getTableName());
      let lockModeStr       = (lockMode.mode) ? lockMode.mode : 'ACCESS EXCLUSIVE';

      if (!lockMode.mode) {
        if (lockMode.read === true && lockMode.write === false)
          lockModeStr = 'EXCLUSIVE';
        else if (lockMode.read === false || lockMode.write === false)
          lockModeStr = 'ACCESS SHARE';
      }

      lockStatement = `LOCK ${(lockMode.dependents === false) ? 'ONLY ' : 'TABLE '} ${escapedTableName} IN ${lockModeStr} MODE${(lockMode.noWait === true) ? ' NOWAIT' : ''}`;
    }

    if (!inheritedThis.inTransaction) {
      client = inheritedThis.inTransaction = await inheritedThis.getPoolConnection();

      let beginSuccess = false;

      try {
        await inheritedThis.query(`BEGIN${(options.beginArguments) ? ` ${options.beginArguments}` : ''}`, options, client);
        beginSuccess = true;

        if (lockStatement)
          await inheritedThis.query(lockStatement, options, client);

        // TODO: Need to handle "busy" error
      } catch (error) {
        if (beginSuccess)
          await inheritedThis.query('ROLLBACK', options, client);

        // Transaction timeout (deadlock)
        if (error.query && (/LOCK TABLE/).test(error.query.text)) {
          let retryCount = _retryCount || 0;
          if (retryCount < 5) {
            await client.release();
            return this.transaction(callback, _options, retryCount + 1);
          }
        }

        if (options.logger)
          options.logger.error(error);

        await client.release();
        throw error;
      }
    } else {
      client = inheritedThis.inTransaction;

      savePointName = inheritedThis.generateSavePointName();
      inheritedThis.savePointName = savePointName;
      inheritedThis.isSavePoint = true;

      await inheritedThis.query(`SAVEPOINT ${savePointName}`, options, client);
    }

    try {
      let result = await inheritedThis.createContext(callback, inheritedThis, inheritedThis);

      if (savePointName)
        await inheritedThis.query(`RELEASE SAVEPOINT ${savePointName}`, options, client);
      else
        await inheritedThis.query('COMMIT', options, client);

      return result;
    } catch (error) {
      if (savePointName)
        await inheritedThis.query(`ROLLBACK TO SAVEPOINT ${savePointName}`, options, client);
      else
        await inheritedThis.query('ROLLBACK', options, client);

      if (options.logger)
        options.logger.error(error);

      throw error;
    } finally {
      if (!savePointName && client)
        await client.release();
    }
  }

  async truncate(Model, options) {
    let queryGenerator  = this.getQueryGenerator();
    let sqlStatement    = queryGenerator.generateTruncateTableStatement(Model, options);

    return await this.query(sqlStatement, options);
  }

  _intTypeToSerial(type) {
    let size = type.length;
    if (size == null)
      size = 4;

    if (size <= 2)
      return 'SMALLSERIAL';
    else if (size <= 4)
      return 'SERIAL';
    else
      return 'BIGSERIAL';
  }

  _bigintTypeToString(type, _options) {
    let options = _options || {};

    if (options.createTable && options.defaultValue === 'AUTOINCREMENT')
      return this._intTypeToSerial(type);

    return 'BIGINT';
  }

  _integerTypeToString(type, _options) {
    let options = _options || {};

    if (options.createTable && options.defaultValue === 'AUTOINCREMENT')
      return this._intTypeToSerial(type);

    return 'INTEGER';
  }

  // eslint-disable-next-line no-unused-vars
  _blobTypeToString(type) {
    return 'BYTEA';
  }

  // eslint-disable-next-line no-unused-vars
  _dateTypeToString(type) {
    return 'BIGINT';
  }

  // eslint-disable-next-line no-unused-vars
  _datetimeTypeToString(type) {
    return 'BIGINT';
  }

  // eslint-disable-next-line no-unused-vars
  _numericTypeToString(type) {
    return `NUMERIC(${type.precision}, ${type.scale})`;
  }

  // eslint-disable-next-line no-unused-vars
  _realTypeToString(type) {
    if ((type.length || 0) <= 4)
      return 'REAL';
    else
      return 'DOUBLE PRECISION';
  }

  async average(_queryEngine, _field, options) {
    let result = await super.average(_queryEngine, _field, options);

    if (typeof result === 'string')
      result = parseFloat(result);

    return result;
  }

  async count(_queryEngine, _field, options) {
    let result = await super.count(_queryEngine, _field, options);

    if (typeof result === 'string')
      result = parseInt(result, 10);

    return result;
  }

  async min(_queryEngine, _field, options) {
    let result = await super.min(_queryEngine, _field, options);

    if (typeof result === 'string')
      result = parseFloat(result);

    return result;
  }

  async max(_queryEngine, _field, options) {
    let result = await super.max(_queryEngine, _field, options);

    if (typeof result === 'string')
      result = parseFloat(result);

    return result;
  }

  async sum(_queryEngine, _field, options) {
    let result = await super.sum(_queryEngine, _field, options);

    if (typeof result === 'string')
      result = parseFloat(result);

    return result;
  }
}

module.exports = PostgreSQLConnection;
