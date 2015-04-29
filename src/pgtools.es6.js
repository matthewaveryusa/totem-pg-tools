'use strict';

/**
 * @module libs/pgtools
 */
const _ = require('lodash'),
  tools = require('totem-tools'),
  log = require('totem-log'),
  pg = require('pg-db')();

/**
 * transform the list of fields into quoted fields that can be inserted into a query
 * @param {array} arr the list of fields
 * @returns {array} transformed fields
 */
exports.fields = function(arr) {
  return arr.map(function(field){ return ''+field+''; });
};

/**
 * transform an array of fields into named-parameter format.
 * @param {array} arr the array of fields
 * @returns {array} transformed fields
 */
exports.namedParams = function (arr) {
  return arr.map(function(field){ return ':' + field; });
};

/**
 * transform an array of fields into a list of arguments for a function.
 * @param {array} arr the array of fields
 * @returns {array} transformed fields
 */
exports.functionParams = function(arr) {
  return arr.map(function(field){ return '_' + field + ' := :' + field; });
};

/**
 * transform an array of fields into a list of update assignments.
 * @param {array} data the array of fields that were requested for update
 * @param {array} fields the set of updateable fields
 * @returns {array}
 */
exports.updateParams = function(data,fields) {
  const params = _.intersection(_.keys(data),fields);
  return params.map(function(field){ return `${field} = :${field}`; }).join(',');
};

class DBError extends Error {
 constructor(code,message,query,data) {
   super();
   this.code = code || -1;
   this.message = message;
   this.query = query;
   this.data= JSON.stringify(data);
 }

 toString() {
  return `code=${this.code} message=${this.message} query=${this.query} data=${this.data}`;
 }
};

exports.DBError = DBError;

exports.makeSelection = function (selection) {
  let arr = [];
  _.forEach(selection,function (val,key){
      arr.push(val + ' AS "'+ key +'"');
  });
  return arr.join(',');
};

function wrapError(err, query, data) {
  var errorEnumIndex;
  if( err.code ) {
    if ( err.constraint) {
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
  } else if (_.startsWith(err.message,'No value found for parameter: ')) {
    return new DBError(err.code, err.message, query, data);
  }
  return new DBError(err.code, err.message, query, data);
}

function makeQueryCallback(queryTag,data,callback){
  const start = Date.now();
  return function (err,results){
    log.metric({'time': Date.now() - start,'data':JSON.stringify(data)},queryTag);
    if(err) {
      err = wrapError(err,queryTag,data);
    }
    callback(err,results);
  };
}

function makeSensitiveQueryCallback(queryTag,data,callback){
  const start = Date.now();
  return function (err,results){
    log.metric({'time': Date.now() - start},queryTag);
    if(err) {
      err = wrapError(err,queryTag,{"sensitive_data_removed":null});
    }
    callback(err,results);
  };
}

exports.query = function(query,data,callback) {
  pg.query(query[1],data,makeQueryCallback(query[0],data,callback));
};

exports.tx = pg.tx;

exports.queryOne = function(query,data,callback) {
  pg.queryOne(query[1],data,makeQueryCallback(query[0],data,callback));
};

exports.update = function(query,data,callback) {
  pg.update(query[1],data,makeQueryCallback(query[0],data,callback));
};

//sensitive queries don't have data attached
exports.sensitiveQuery = function(query,data,callback) {
  pg.query(query[1],data,makeSensitiveQueryCallback(query[0],data,callback));
};

exports.sensitiveQueryOne = function(query,data,callback) {
  pg.queryOne(query[1],data,makeSensitiveQueryCallback(query[0],data,callback));
};

exports.sensitiveUpdate = function(query,data,callback) {
  pg.update(query[1],data,makeSensitiveQueryCallback(query[0],data,callback));
};

exports.upsert = function(updateQuery,insertQuery,data,callback) {
  const query = `WITH upsert AS (${updateQuery[1]} RETURNING *) ${insertQuery[1]} WHERE NOT EXISTS (SELECT * FROM upsert);`;
  pg.update(query,data,makeQueryCallback(updateQuery[0],data,callback));
};

exports.sensitiveUpsert = function(updateQuery,insertQuery,data,callback) {
  const query = `WITH upsert AS (${updateQuery[1]} RETURNING *) ${insertQuery[1]} WHERE NOT EXISTS (SELECT * FROM upsert);`;
  pg.update(query[1],data,makeSensitiveQueryCallback(updateQuery[0],data,callback));
};
