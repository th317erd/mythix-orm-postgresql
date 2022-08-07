'use strict';

const { Utils }                 = require('mythix-orm');
const { PostgreSQLConnection }  = require('../../../lib');
const DBCredentials             = require('../../support/db-credentials.js');

async function createConnection() {
  const createTable = async (connection, Model, options) => {
    await connection.dropTable(Model, Object.assign({}, options || {}, { ifExists: true, cascade: true }));
    return await connection.createTable(Model, options);
  };

  let connection = new PostgreSQLConnection(Object.assign(
    {
      maxPoolConnections:     2,
      statementTimeout:       1000,
      queryTimeout:           1000,
      connectionTimeout:      1000,
      idleTransactionTimeout: 1000,
      idleConnectionTimeout:  1000,
    },
    DBCredentials,
    {
      bindModels: false,
      models:     require('../../support/models'),
    },
  ));

  await connection.start();

  let models      = connection.getModels();
  let modelNames  = Object.keys(models);

  modelNames = Utils.sortModelNamesByCreationOrder(connection, modelNames);

  for (let i = 0, il = modelNames.length; i < il; i++) {
    let modelName = modelNames[i];
    let model     = models[modelName];

    await createTable(connection, model);
  }

  return Object.assign({}, models, { connection });
}

async function truncateTables(connection) {
  let models  = connection.getModels();
  let keys    = Object.keys(models);

  for (let i = 0, il = keys.length; i < il; i++) {
    let key   = keys[i];
    let model = models[key];

    try {
      await await connection.truncate(model);
    } catch (error) {
      console.error('TRUNCATE TABLE FAILED: ', error);
      throw error;
    }
  }
}

module.exports = {
  createConnection,
  truncateTables,
};
