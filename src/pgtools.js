'use strict';

var _classCallCheck = function (instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError('Cannot call a class as a function'); } };

var _createClass = (function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ('value' in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; })();

var _inherits = function (subClass, superClass) { if (typeof superClass !== 'function' && superClass !== null) { throw new TypeError('Super expression must either be null or a function, not ' + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) subClass.__proto__ = superClass; };

/**
 * @module libs/pgtools
 */
var _ = require('lodash'),
    tools = require('totem-tools'),
    log = require('totem-log'),
    pg = require('pg-db')();

/**
 * transform the list of fields into quoted fields that can be inserted into a query
 * @param {array} arr the list of fields
 * @returns {array} transformed fields
 */
exports.fields = function (arr) {
  return arr.map(function (field) {
    return '' + field + '';
  });
};

/**
 * transform an array of fields into named-parameter format.
 * @param {array} arr the array of fields
 * @returns {array} transformed fields
 */
exports.namedParams = function (arr) {
  return arr.map(function (field) {
    return ':' + field;
  });
};

/**
 * transform an array of fields into a list of arguments for a function.
 * @param {array} arr the array of fields
 * @returns {array} transformed fields
 */
exports.functionParams = function (arr) {
  return arr.map(function (field) {
    return '_' + field + ' := :' + field;
  });
};

/**
 * transform an array of fields into a list of update assignments.
 * @param {array} data the array of fields that were requested for update
 * @param {array} fields the set of updateable fields
 * @returns {array}
 */
exports.updateParams = function (data, fields) {
  var params = _.intersection(_.keys(data), fields);
  return params.map(function (field) {
    return '' + field + ' = :' + field;
  }).join(',');
};

var DBError = (function (_Error) {
  function DBError(code, message, query, data) {
    _classCallCheck(this, DBError);

    var _this = new _Error();

    _this.__proto__ = DBError.prototype;

    _this.code = code || -1;
    _this.message = message;
    _this.query = query;
    _this.data = JSON.stringify(data);
    return _this;
  }

  _inherits(DBError, _Error);

  _createClass(DBError, [{
    key: 'toString',
    value: function toString() {
      return 'code=' + this.code + ' message=' + this.message + ' query=' + this.query + ' data=' + this.data;
    }
  }]);

  return DBError;
})(Error);

;

exports.DBError = DBError;

exports.makeSelection = function (selection) {
  var arr = [];
  _.forEach(selection, function (val, key) {
    arr.push(val + ' AS "' + key + '"');
  });
  return arr.join(',');
};

function wrapError(err, query, data) {
  var errorEnumIndex;
  if (err.code) {
    if (err.constraint) {
      errorEnumIndex = err.constraint.indexOf('$');
      /* istanbul ignore else */
      if (errorEnumIndex !== -1) {
        return new tools.ClientError(422, err.constraint.substring(errorEnumIndex + 1));
      }
    }

    if (err.code === 'TOTEM') {
      errorEnumIndex = err.message.indexOf('$');
      /* istanbul ignore else */
      if (errorEnumIndex !== -1) {
        return new tools.ClientError(422, err.message.substring(errorEnumIndex + 1));
      }
    }
  } else if (_.startsWith(err.message, 'No value found for parameter: ')) {
    return new DBError(err.code, err.message, query, data);
  }
  return new DBError(err.code, err.message, query, data);
}

function makeQueryCallback(queryTag, data, callback) {
  var start = Date.now();
  return function (err, results) {
    log.metric({ time: Date.now() - start, data: JSON.stringify(data) }, queryTag);
    if (err) {
      err = wrapError(err, queryTag, data);
    }
    callback(err, results);
  };
}

function makeSensitiveQueryCallback(queryTag, data, callback) {
  var start = Date.now();
  return function (err, results) {
    log.metric({ time: Date.now() - start }, queryTag);
    if (err) {
      err = wrapError(err, queryTag, { sensitive_data_removed: null });
    }
    callback(err, results);
  };
}

exports.query = function (query, data, callback) {
  pg.query(query[1], data, makeQueryCallback(query[0], data, callback));
};

exports.tx = pg.tx;

exports.queryOne = function (query, data, callback) {
  pg.queryOne(query[1], data, makeQueryCallback(query[0], data, callback));
};

exports.update = function (query, data, callback) {
  pg.update(query[1], data, makeQueryCallback(query[0], data, callback));
};

//sensitive queries don't have data attached
exports.sensitiveQuery = function (query, data, callback) {
  pg.query(query[1], data, makeSensitiveQueryCallback(query[0], data, callback));
};

exports.sensitiveQueryOne = function (query, data, callback) {
  pg.queryOne(query[1], data, makeSensitiveQueryCallback(query[0], data, callback));
};

exports.sensitiveUpdate = function (query, data, callback) {
  pg.update(query[1], data, makeSensitiveQueryCallback(query[0], data, callback));
};

exports.upsert = function (updateQuery, insertQuery, data, callback) {
  var query = 'WITH upsert AS (' + updateQuery[1] + ' RETURNING *) ' + insertQuery[1] + ' WHERE NOT EXISTS (SELECT * FROM upsert);';
  pg.update(query, data, makeQueryCallback(updateQuery[0], data, callback));
};

exports.sensitiveUpsert = function (updateQuery, insertQuery, data, callback) {
  var query = 'WITH upsert AS (' + updateQuery[1] + ' RETURNING *) ' + insertQuery[1] + ' WHERE NOT EXISTS (SELECT * FROM upsert);';
  pg.update(query[1], data, makeSensitiveQueryCallback(updateQuery[0], data, callback));
};
