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
    return this.generateReturningClause(Model, model, options, context);
  }

  generateUpdateStatementTail(Model, model, options, context) {
    return this.generateReturningClause(Model, model, options, context);
  }

  generateColumnDeclarationStatement(Model, field, _options) {
    let options = _options || {};
    return super.generateColumnDeclarationStatement(Model, field, { ...options, noAutoIncrementDefault: true });
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

  generateOrderClause(queryEngine, _options) {
    if (!queryEngine || typeof queryEngine.getOperationContext !== 'function')
      return '';

    let context = queryEngine.getOperationContext();
    let result  = super.generateOrderClause(queryEngine, this.stackAssign(_options || {}, { rawOrder: true }));

    if (Nife.isNotEmpty(result) && context.distinct) {
      let distinctValue = context.distinct.getField(this.connection);
      if (distinctValue) {
        let valueStr;

        if (LiteralBase.isLiteral(distinctValue))
          valueStr = distinctValue.toString(this.connection, { isOrder: true });
        else if (typeof distinctValue.isField === 'function' && distinctValue.isField(distinctValue))
          valueStr = this.getEscapedColumnName(distinctValue.Model, distinctValue);

        if (valueStr)
          result = [ `${valueStr} ASC` ].concat(result);
      }
    }

    if (Nife.isEmpty(result))
      return '';

    return `ORDER BY ${result.join(',')}`;
  }
}

module.exports = PostgreSQLQueryGenerator;
