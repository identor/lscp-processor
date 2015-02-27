var assert = require('assert');
var queries = require('../lib/queries.js');
var DbUtils = require('../lib/db-utils.js').DbUtils;
var common = require('./common.js');

describe('Call Count Tests', function () {
  var scores;
  beforeEach(function (done) {
    var finished = function () {
      scores = common.scores;
      done();
    };
    common.initDb(finished);
  });
  it('should invoke the callProcessedCounts without errors.', function (done) {
    var finished = function (callsProcessed) {
      done();
    };
    queries.callsProcessedCount(scores, finished);
  });
  it('should aggregate the scores in CPone.csv.', function (done) {
    var finished = function (callsProcessed) {
      assert.equal(callsProcessed.length, 26);
      done();
    };
    var saved = function () {
      queries.callsProcessedCount(scores, finished);
    };
    DbUtils.saveToDb(common.cpOne, scores, common.formatting, saved);
  });
});
