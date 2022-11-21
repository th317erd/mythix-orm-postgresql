/* eslint-disable indent */
/* eslint-disable no-magic-numbers */

'use strict';

/* global describe, expect, beforeAll, afterEach */

const { Literals } = require('mythix-orm');

const {
  createConnection,
  truncateTables,
} = require('../postgresql-connection-helper');

const { createRunners } = require('../../../support/test-helpers');

describe('PostgreSQLQueryGenerator', () => {
  let connection;
  let User;
  let Role;

  // eslint-disable-next-line no-unused-vars
  const { it, fit } = createRunners(() => connection);

  beforeAll(async () => {
    try {
      let setup = await createConnection();
      connection = setup.connection;

      let models = connection.getModels();
      User = models.User;
      Role = models.Role;
    } catch (error) {
      console.error('Error in beforeAll: ', error);
    }
  });

  afterEach(async () => {
    await truncateTables(connection);
  });

  describe('getProjectedFields', () => {
    it('can generate escaped field list from projected fields', () => {
      let queryGenerator  = connection.getQueryGenerator();
      let fieldList       = queryGenerator.getProjectedFields(User.where.AND.Role.PROJECT('*'));

      expect(fieldList).toEqual([
        'users.id AS "User:id"',
        'users."firstName" AS "User:firstName"',
        'users."lastName" AS "User:lastName"',
        'users."primaryRoleID" AS "User:primaryRoleID"',
        'roles.id AS "Role:id"',
        'roles.name AS "Role:name"',
      ]);
    });

    it('can set projected fields', () => {
      let queryGenerator  = connection.getQueryGenerator();
      let fieldList       = queryGenerator.getProjectedFields(User.where.PROJECT('User:id'));
      expect(fieldList).toEqual([
        'users.id AS "User:id"',
      ]);
    });

    it('can set projected fields with literals', () => {
      let queryGenerator  = connection.getQueryGenerator();
      let fieldList       = queryGenerator.getProjectedFields(User.where.PROJECT('User:id', new Literals.Literal('DISTINCT "users"."firstName" AS "User:firstName"')));

      expect(fieldList).toEqual([
        'users.id AS "User:id"',
        'DISTINCT "users"."firstName" AS "User:firstName"',
      ]);
    });

    it('can subtract from projected fields', () => {
      let queryGenerator  = connection.getQueryGenerator();
      let fieldList       = queryGenerator.getProjectedFields(User.where.PROJECT('-User:id'));
      expect(fieldList).toEqual([
        'users."firstName" AS "User:firstName"',
        'users."lastName" AS "User:lastName"',
        'users."primaryRoleID" AS "User:primaryRoleID"',
      ]);

      fieldList = queryGenerator.getProjectedFields(User.where.PROJECT('*', '-User:id'));
      expect(fieldList).toEqual([
        'users."firstName" AS "User:firstName"',
        'users."lastName" AS "User:lastName"',
        'users."primaryRoleID" AS "User:primaryRoleID"',
      ]);
    });
  });
});
