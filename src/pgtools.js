'use strict';
/**
 * @module libs/pgtools
 */

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


