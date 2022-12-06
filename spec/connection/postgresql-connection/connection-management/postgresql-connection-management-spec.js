/* eslint-disable no-magic-numbers */

'use strict';

/* global describe, expect, beforeEach */

const { Literals }              = require('mythix-orm');
const { PostgreSQLConnection }  = require('../../../../lib');
const { createRunners }         = require('../../../support/test-helpers');
const DBCredentials             = require('../../../support/db-credentials.js');

describe('PostgreSQLConnection', () => {
  describe('connection management', () => {
    let connection;
    let User;
    let Role;

    // eslint-disable-next-line no-unused-vars
    const { it, fit } = createRunners(() => connection);

    beforeEach(async () => {
      connection = new PostgreSQLConnection({
        ...DBCredentials,
        bindModels:         false,
        maxPoolConnections: 2,
        models:             require('../../../support/models'),
      });

      let models = connection.getModels();

      User = models.User;
      Role = models.Role;
    });

    describe('getLiteralClassByName', () => {
      it('can return literal class', () => {
        expect(PostgreSQLConnection.getLiteralClassByName('distinct')).toBe(PostgreSQLConnection.Literals.DistinctLiteral);
        expect(PostgreSQLConnection.getLiteralClassByName('DISTINCT')).toBe(PostgreSQLConnection.Literals.DistinctLiteral);
        expect(PostgreSQLConnection.getLiteralClassByName('Distinct')).toBe(PostgreSQLConnection.Literals.DistinctLiteral);
        expect(PostgreSQLConnection.getLiteralClassByName('literal')).toBe(PostgreSQLConnection.Literals.Literal);
        expect(PostgreSQLConnection.getLiteralClassByName('LITERAL')).toBe(PostgreSQLConnection.Literals.Literal);
        expect(PostgreSQLConnection.getLiteralClassByName('base')).toBe(PostgreSQLConnection.Literals.LiteralBase);
      });
    });

    describe('Literal', () => {
      it('can instantiate a SQL literal', () => {
        expect(PostgreSQLConnection.Literal('distinct', 'User:firstName')).toBeInstanceOf(PostgreSQLConnection.Literals.DistinctLiteral);
      });

      it('can stringify a literal to SQL', () => {
        let literal = PostgreSQLConnection.Literal('distinct', 'User:firstName');
        expect(literal.toString(connection)).toEqual('DISTINCT ON(users."firstName")');
      });

      it('will stringify to class name if no connection given', () => {
        let literal = PostgreSQLConnection.Literal('distinct', 'User:firstName');
        expect(literal.toString()).toEqual('DistinctLiteral {}');
      });
    });

    describe('escape', () => {
      it('can escape a string value', () => {
        expect(connection.escape(User.fields.id, 'test "hello";')).toEqual('\'test "hello";\'');
      });

      it('can escape a integer value', () => {
        expect(connection.escape(User.fields.id, 10)).toEqual('\'10\'');
        expect(connection.escape(User.fields.id, -10)).toEqual('\'-10\'');
      });

      it('can escape a number value', () => {
        expect(connection.escape(User.fields.id, 10.345)).toEqual('\'10.345\'');
        expect(connection.escape(User.fields.id, -10.345)).toEqual('\'-10.345\'');
      });

      it('can escape a boolean value', () => {
        expect(connection.escape(User.fields.id, true)).toEqual('TRUE');
        expect(connection.escape(User.fields.id, false)).toEqual('FALSE');
      });

      it('should not escape a literal value', () => {
        expect(connection.escape(User.fields.id, new Literals.Literal('!$#%'))).toEqual('!$#%');
      });
    });

    describe('escapeID', () => {
      it('can escape a string value', () => {
        expect(connection.escapeID('test.derp')).toEqual('"test.derp"');
      });

      it('should not escape a literal value', () => {
        expect(connection.escapeID(new Literals.Literal('!$#%'))).toEqual('!$#%');
      });
    });

    describe('dialect', () => {
      it('can return dialect', () => {
        expect(PostgreSQLConnection.dialect).toEqual('postgresql');
        expect(connection.dialect).toEqual('postgresql');
      });
    });

    describe('start', () => {
      it('can initiate a DB connection', async () => {
        expect(connection.pool).toBe(null);
        await connection.start();
        expect(connection.pool).not.toBe(null);
      });
    });

    describe('stop', () => {
      it('can shutdown a DB connection', async () => {
        expect(connection.pool).toBe(null);
        await connection.start();
        expect(connection.pool).not.toBe(null);

        await connection.stop();
        expect(connection.pool).toBe(null);
      });
    });

    describe('generateSavePointName', () => {
      it('can generate a save point name', async () => {
        expect(connection.generateSavePointName()).toMatch(/SP[A-P]{32}/);
      });
    });
  });
});
