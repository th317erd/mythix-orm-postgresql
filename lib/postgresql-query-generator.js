'use strict';

const Nife                      = require('nife');
const { Literals }              = require('mythix-orm');
const { SQLQueryGeneratorBase } = require('mythix-orm-sql-base');

const LiteralBase = Literals.LiteralBase;

class PostgreSQLQueryGenerator extends SQLQueryGeneratorBase {
  // eslint-disable-next-line no-unused-vars
  generateSQLJoinTypeFromQueryEngineJoinType(joinType, outer, options) {
    if (!joinType || joinType === 'inner')
      return 'INNER JOIN';
    else if (joinType === 'left')
      return 'LEFT JOIN';
    else if (joinType === 'cross')
      return 'CROSS JOIN';

    return joinType;
  }

  generateForeignKeyConstraint(field, type) {
    let options     = type.getOptions();
    let targetModel = type.getTargetModel();
    let targetField = type.getTargetField();

    let sqlParts  = [
      'FOREIGN KEY(',
      this.escapeID(field.columnName),
      ') REFERENCES ',
      this.escapeID(targetModel.getTableName(this.connection)),
      '(',
      this.escapeID(targetField.columnName),
      ')',
    ];

    if (options.onDelete) {
      sqlParts.push(' ');
      sqlParts.push(`ON DELETE ${options.onDelete.toUpperCase()}`);
    }

    if (options.onUpdate) {
      sqlParts.push(' ');
      sqlParts.push(`ON UPDATE ${options.onUpdate.toUpperCase()}`);
    }

    if (options.deferred !== false) {
      sqlParts.push(' ');

      if (Nife.instanceOf(options.deferred, 'string'))
        sqlParts.push(options.deferred);
      else
        sqlParts.push('DEFERRABLE INITIALLY DEFERRED');
    }

    return sqlParts.join('');
  }

  // eslint-disable-next-line no-unused-vars
  generateCreateTableStatementInnerTail(Model, options) {
    let fieldParts = [];

    Model.iterateFields(({ field }) => {
      if (field.type.isVirtual())
        return;

      if (field.type.isForeignKey()) {
        let result = this.generateForeignKeyConstraint(field, field.type);
        if (result)
          fieldParts.push(result);

        return;
      }
    });

    return fieldParts;
  }

  generateInsertStatementTail(Model, model, options, context) {
    return this._collectReturningFields(Model, model, options, context);
  }

  generateUpdateStatementTail(Model, model, options, context) {
    return this._collectReturningFields(Model, model, options, context);
  }

  generateCreateTableStatement(Model, _options) {
    let options = _options || {};
    let fieldParts = [];

    Model.iterateFields(({ field, fieldName }) => {
      if (field.type.isVirtual())
        return;

      let columnName      = field.columnName || fieldName;
      let constraintParts = [];

      let defaultValue = this.getFieldDefaultValue(field, fieldName, { remoteOnly: true });

      if (field.primaryKey) {
        if (field.primaryKey instanceof LiteralBase)
          constraintParts.push(field.primaryKey.toString(this.connection));
        else
          constraintParts.push('PRIMARY KEY');

        if (defaultValue !== '@@@AUTOINCREMENT@@@')
          constraintParts.push('NOT NULL');
      } else {
        if (field.unique) {
          if (field.unique instanceof LiteralBase)
            constraintParts.push(field.unique.toString(this.connection));
          else
            constraintParts.push('UNIQUE');
        }

        if (field.allowNull === false)
          constraintParts.push('NOT NULL');
      }

      if (defaultValue !== undefined && defaultValue !== '@@@AUTOINCREMENT@@@')
        constraintParts.push(defaultValue);

      constraintParts = constraintParts.join(' ');
      if (Nife.isNotEmpty(constraintParts))
        constraintParts = ` ${constraintParts}`;

      fieldParts.push(`  ${this.escapeID(columnName)} ${field.type.toConnectionType(this.connection, { createTable: true, defaultValue })}${constraintParts}`);
    });

    let ifNotExists = 'IF NOT EXISTS ';
    if (options.ifNotExists === false)
      ifNotExists = '';

    let trailingParts = Nife.toArray(this.generateCreateTableStatementInnerTail(Model, options)).filter(Boolean);
    if (Nife.isNotEmpty(trailingParts))
      fieldParts = fieldParts.concat(trailingParts.map((part) => `  ${part.trim()}`));

    let finalStatement = `CREATE TABLE ${ifNotExists}${this.escapeID(Model.getTableName(this.connection))} (${fieldParts.join(',\n')}\n);`;
    return finalStatement;
  }

  generateTruncateTableStatement(Model, _options) {
    let options           = _options || {};
    let escapedTableName  = this.escapeID(Model.getTableName(this.connection));

    return `TRUNCATE TABLE${(options.onlySpecifiedTable === true) ? ' ONLY' : ''} ${escapedTableName}${(options.continueIdentity === true) ? ' CONTINUE IDENTITY' : ' RESTART IDENTITY'}${(options.cascade === false) ? ' RESTRICT' : ' CASCADE'}`;
  }

  generateSelectQueryOperatorFromQueryEngineOperator(queryPart, operator, value, valueIsReference, options) {
    let sqlOperator = super.generateSelectQueryOperatorFromQueryEngineOperator(queryPart, operator, value, valueIsReference, options);

    if (sqlOperator === 'LIKE' && queryPart.caseSensitive !== true)
      sqlOperator = 'ILIKE';
    else if (sqlOperator === 'NOT LIKE' && queryPart.caseSensitive !== true)
      sqlOperator = 'NOT ILIKE';

    return sqlOperator;
  }
}

module.exports = PostgreSQLQueryGenerator;
