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

const DEFAULT_TIMEOUT_MS = 5000;

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

class PostgreSQLConnection extends SQLConnectionBase {
  static dialect = 'postgresql';

  static DefaultQueryGenerator = PostgreSQLQueryGenerator;

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
        statement_timeout:                    options.statementTimeout || DEFAULT_TIMEOUT_MS,
        query_timeout:                        options.queryTimeout || DEFAULT_TIMEOUT_MS,
        connectionTimeoutMillis:              options.connectionTimeout || 60000,
        idle_in_transaction_session_timeout:  options.idleTransactionTimeout,
        max:                                  options.maxPoolConnections,
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
    // Not supported in PostgreSQL
    return;
  }

  async exec(...args) {
    return await this.query(...args);
  }

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
        console.log('QUERY: ', sql.text);

      let result;

      if (Nife.isNotEmpty(options.parameters))
        result = await client.query(sql, options.parameters);
      else
        result = await client.query(sql);

      if (client && hasOwnClient)
        await client.release();

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

  async transaction(callback, _options, _retryCount) {
    let options       = _options || {};
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
      let result = await await inheritedThis.createContext(callback, inheritedThis, inheritedThis);

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

      throw error;
    } finally {
      if (!savePointName && client)
        await client.release();
    }
  }

  formatResultsResponse(sqlStatement, result) {
    if (!result.rows || !result.fields)
      return result;

    return {
      rows:     result.rows,
      columns:  result.fields.map((field) => field.name),
    };
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
