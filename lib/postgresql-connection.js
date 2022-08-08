/* eslint-disable no-magic-numbers */
/* eslint-disable camelcase */

'use strict';

const Nife                      = require('nife');
const moment                    = require('mythix-orm/node_modules/moment');
const PG                        = require('pg');
const PGFormat                  = require('pg-format');
const { Literals }              = require('mythix-orm');
const { SQLConnectionBase }     = require('mythix-orm-sql-base');
const PostgreSQLQueryGenerator  = require('./postgresql-query-generator');

class PostgreSQLConnection extends SQLConnectionBase {
  static dialect = 'postgresql';

  constructor(_options) {
    super(_options);

    this.setQueryGenerator(new PostgreSQLQueryGenerator(this));

    Object.defineProperties(this, {
      'pool': {
        writable:     true,
        enumberable:  false,
        configurable: true,
        value:        null,
      },
    });
  }

  isStarted() {
    return !!this.pool;
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
        statement_timeout:                    options.statementTimeout,
        query_timeout:                        options.queryTimeout,
        connectionTimeoutMillis:              options.connectionTimeout,
        idle_in_transaction_session_timeout:  options.idleTransactionTimeout,
        idleTimeoutMillis:                    options.idleConnectionTimeout,
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
        return new Literals.Literal('@@@AUTOINCREMENT@@@', { noDefaultStatementOnCreateTable: true, remote: true });
      case 'DATETIME_NOW':
        return new Literals.Literal('(NOW() AT TIME ZONE(\'UTC\'))', { escape: false, remote: true });
      case 'DATE_NOW':
        return new Literals.Literal('(NOW() AT TIME ZONE(\'UTC\'))', { escape: false, remote: true });
      case 'DATETIME_NOW_LOCAL':
        return moment().toDate();
      case 'DATE_NOW_LOCAL':
        return moment().startOf('day').toDate();
      default:
        return type;
    }
  }

  async exec(...args) {
    return await this.query(...args);
  }

  async query(_sql, _options) {
    let sql = _sql;
    if (!sql)
      return;

    if (Nife.instanceOf(sql, 'string'))
      sql = { text: sql, rowMode: 'array' };
    else if (Nife.instanceOf(sql, 'object'))
      sql = Object.assign({ text: sql, rowMode: 'array' }, sql);

    let options       = _options || {};
    let logger        = options.logger || (this.getOptions().logger);
    let inTransaction = false;
    let client;

    try {
      client = options.connection || this.inTransaction;
      if (!client)
        client = await this.pool.connect();
      else
        inTransaction = true;

      if (logger && sql.text)
        console.log('QUERY: ', sql.text);

      let result;

      if (Nife.isNotEmpty(options.parameters))
        result = await client.query(sql, options.parameters);
      else
        result = await client.query(sql);

      return this.formatResultsResponse(sql, result);
    } catch (error) {
      if (logger) {
        logger.error(error);
        logger.error('QUERY: ', sql);
      }

      throw error;
    } finally {
      if (client && !inTransaction)
        await client.release();
    }
  }

  async transaction(callback, _options) {
    let options       = _options || {};
    let inheritedThis = Object.create(options.connection || this);
    let savePointName;
    let client;

    if (!inheritedThis.inTransaction) {
      client = options.connection;
      if (!client)
        client = await this.pool.connect();

      inheritedThis.inTransaction = client;

      try {
        await inheritedThis.query(`BEGIN${(options.mode) ? ` ${options.mode}` : ''}`);
      } catch (error) {
        if (!options.connection)
          await client.release();

        throw error;
      }
    } else {
      savePointName = this.generateSavePointName();
      inheritedThis.savePointName = savePointName;
      inheritedThis.isSavePoint = true;

      await inheritedThis.query(`SAVEPOINT ${savePointName}`);
    }

    try {
      let result = await callback.call(inheritedThis, inheritedThis);

      if (savePointName)
        await inheritedThis.query(`RELEASE SAVEPOINT ${savePointName}`);
      else
        await inheritedThis.query('COMMIT');

      return result;
    } catch (error) {
      if (savePointName)
        await inheritedThis.query(`ROLLBACK TO SAVEPOINT ${savePointName}`);
      else if (inheritedThis.inTransaction)
        await inheritedThis.query('ROLLBACK');

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

    return await this.query(sqlStatement);
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

    if (options.createTable && options.defaultValue === '@@@AUTOINCREMENT@@@')
      return this._intTypeToSerial(type);

    return 'BIGINT';
  }

  _integerTypeToString(type, _options) {
    let options = _options || {};

    if (options.createTable && options.defaultValue === '@@@AUTOINCREMENT@@@')
      return this._intTypeToSerial(type);

    return 'INTEGER';
  }

  _blobTypeToString(type) {
    return 'BYTEA';
  }

  _datetimeTypeToString(type) {
    return 'TIMESTAMP';
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
