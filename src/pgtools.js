'use strict';

/**
 * @module libs/pgtools
 */
const _ = require('lodash'),
      log = require('tracer').console(),
      pg = require('pg-db')();

/**
 * transform the list of fields into quoted fields that can be inserted into a query
 * @param {array} arr the list of fields
 * @returns {array} transformed fields
 */
function fields(arr) {
  return arr.map(function(field){ return ''+field+''; });
}
exports.fields = fields;

/**
 * transform an array of fields into named-parameter format.
 * @param {array} arr the array of fields
 * @returns {array} transformed fields
 */
function namedParams(arr) {
  return arr.map(function(field){ return ':' + field; });
}
exports.namedParams = namedParams;

/**
 * transform an array of fields into a list of arguments for a function.
 * @param {array} arr the array of fields
 * @returns {array} transformed fields
 */
function functionParams(arr) {
  return arr.map(function(field){ return '_' + field + ' := :' + field; });
}
exports.functionParams = functionParams;

function DBError(status,errorCode,query,data) {
  this.status = status;
  this.errorCode = errorCode;
  this.query = query;
  this.data= data;
}

DBError.prototype.constructor = DEBError;

exports.DBError = DBError;

function makeSelection(selection) {
  let arr = [];
  _.forEach(selection,function selectionCB(val,key){
      arr.push(val + ' AS "'+ key +'"');
  });
  return arr.join(',');
}
exports.makeSelection = makeSelection;

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
        return new tools.ClientError(422, err.constraint.substring(errorEnumIndex + 1));
      }
    }
  } else if (_.startsWith(err.message,'No value found for parameter: ')) {
    return new DBError(err.code, err.message, query, data));
  }
  return new DBError(err.code, err.message, query, data);
}

function makeQueryCallback(req){
  const start = Date.now();
  return function(err,results){
    log.info('metric','query',Date.now() - start,query,data);
    if(err) {
      err = wrapError(err);
    }
    callback(err,results);
  };
}

function makeSensitiveQueryCallback(req){
  const start = Date.now();
  const position = ++req.stats.position;
  return function(err,results){
    log.info('metric','query',Date.now() - start,query);
    if(err) {
      err = wrapError(err);
    }
    callback(err,results);
  };
}

function query(req,query,data,callback) {
  pg.query(query,data,(err,makeQueryCallback);
}

exports.query = query;

function queryOne(req,query,data,callback) {
  pg.queryOne(query,data,(err,makeQueryCallback);
}
exports.queryOne = queryOne;

function update(req,query,data,callback) {
  pg.update(query,data,(err,makeQueryCallback);
}

//sensitive queries don't have data attached
function sensitiveQuery(req,query,data,callback) {
  pg.query(query,data,(err,makeSensitiveQueryCallback);
}
exports.sensitiveQuery = sensitiveQuery;

function sensitiveQueryOne(req,query,data,callback) {
  pg.queryOne(query,data,(err,makeSensitiveQueryCallback);
}
exports.sensitiveQueryOne = sensitiveQueryOne;

function sensitiveUpdate(req,query,data,callback) {
  pg.update(query,data,(err,makeSensitiveQueryCallback);
}
exports.sensitiveUpdate = sensitiveUpdate;
