'use strict';

const { Model, Types } = require('mythix-orm');

class Time extends Model {
  static fields = {
    'id': {
      type:         Types.XID,
      defaultValue: Types.XID.Default.XID,
      allowNull:    false,
      primaryKey:   true,
    },
    'datetime': {
      type:         Types.DATETIME,
      defaultValue: Types.DATETIME.Default.NOW,
      index:        true,
    },
    'datetimeLocal': {
      type:         Types.DATETIME,
      defaultValue: Types.DATETIME.Default.NOW.LOCAL,
      index:        true,
    },
    'date': {
      type:         Types.DATE,
      defaultValue: Types.DATE.Default.NOW,
      index:        true,
    },
    'dateLocal': {
      type:         Types.DATE,
      defaultValue: Types.DATE.Default.NOW.LOCAL,
      index:        true,
    },
    'customDate': {
      type:  Types.DATE,
      index: true,
    },
    'customDateTime': {
      type:  Types.DATETIME,
      index: true,
    },
  };
}

module.exports = Time;
