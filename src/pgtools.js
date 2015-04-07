'use strict';

/**
 * @module libs/pgtools
 */
const _ = require('lodash'),
  log = require('tracer').console(),
  tools = require('totem-tools'),
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

/**
 * transform an array of fields into a list of update assignments.
 * @param {array} data the array of fields that were requested for update
 * @param {array} fields the set of updateable fields
 * @returns {array}
 */
function updateParams(data,fields) {
  const params = _.intersection(_.keys(data),fields);
  return params.map(function(field){ return `${field} = :${field}`; }).join(',');
}
exports.updateParams = updateParams;

function DBError(code,message,query,data) {
  this.code = code || -1;
  this.message = message;
  this.query = query;
  this.data= JSON.stringify(data);
}

DBError.prototype.constructor = DBError;

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
  return function pgQuery(err,results){
    log.info('metric', Date.now() - start,queryTag,JSON.stringify(data));
    if(err) {
      err = wrapError(err,queryTag,data);
    }
    callback(err,results);
  };
}

function makeSensitiveQueryCallback(queryTag,data,callback){
  const start = Date.now();
  return function pgQuery(err,results){
    log.info('metric',Date.now() - start,queryTag);
    if(err) {
      err = wrapError(err,queryTag,data);
    }
    callback(err,results);
  };
}

function query(query,data,callback) {
  pg.query(query[1],data,makeQueryCallback(query[0],data,callback));
}

exports.query = query;

function queryOne(query,data,callback) {
  pg.queryOne(query[1],data,makeQueryCallback(query[0],data,callback));
}
exports.queryOne = queryOne;

function update(query,data,callback) {
  pg.update(query[1],data,makeQueryCallback(query[0],data,callback));
}
exports.update = update;

//sensitive queries don't have data attached
function sensitiveQuery(query,data,callback) {
  pg.query(query[1],data,makeSensitiveQueryCallback(query[0],data,callback));
}
exports.sensitiveQuery = sensitiveQuery;

function sensitiveQueryOne(query,data,callback) {
  pg.queryOne(query[1],data,makeSensitiveQueryCallback(query[0],data,callback));
}
exports.sensitiveQueryOne = sensitiveQueryOne;

function sensitiveUpdate(query,data,callback) {
  pg.update(query[1],data,makeSensitiveQueryCallback(query[0],data,callback));
}
exports.sensitiveUpdate = sensitiveUpdate;
