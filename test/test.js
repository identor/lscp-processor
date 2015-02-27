var assert = require('assert');
var ccpo = require('../lib').CallsProcessed.parseCsv;
var scpm = require('../lib').DbUtils.saveToDb;
var common = require('./common.js');

describe('LSCP Parser', function () {
  it('should define ccpo as a function', function () {
    assert.equal(typeof ccpo, 'function');
  });
  it('should create an array with 30 elements', function (done) {
    var csvPath = 'test/ECPwiththirtyelements.csv';
    console.log('Processing file:', csvPath);
    ccpo(csvPath, common.formatting, function (callsProcessedObjects) {
      assert.equal(callsProcessedObjects.length, 30);
      done();
    });
  });
});

describe('MongoDB data store Tests.', function () {
  var ecpOne = common.ecpOne;
  var ecpTwo = common.ecpTwo;
  var cpOne = common.cpOne;
  var formatting = common.formatting;
  var scores;
  var mongodb;
  //var lcpTwo = '';
  beforeEach(function (done) {
    var finished = function() {
      mongodb = common.mongodb;
      scores = common.scores;
      done();
    };
    common.initDb(finished);
  });
  it('should store the 30 elements contained in ' + ecpOne + '.', function (done) {
    var afterInsert = function (report) {
      console.log(report);
      scores.count(function (err, count) {
        assert.equal(30, count);
        return done();
      });
    };
    scpm(ecpOne, scores, formatting, afterInsert);
  });
  it('should not store objects contained in ' + ecpOne + ' twice.', function (done) {
    var insertCount = 0;
    var afterInsert = function (report) {
      console.log(report);
      ++insertCount;
      if (insertCount === 2) {
        scores.count(function (err, count) {
          assert.equal(30, count);
          return done();
        });
      }
    };
    scpm(ecpOne, scores, formatting, afterInsert);
    scpm(ecpOne, scores, formatting, afterInsert);
  });
  it('should store a record in weirdScores.', function (done) {
    var afterInsert = function (report) {
      console.log(report);
      var selector = { _id: 'baed5aa21a92467021b0faf82837e9eaf93c4509' };
      scores.findOne(selector,
          function (err, obj) {
        if (err) throw err;
        assert.equal(obj._id, selector._id);
        return done();
      });
    };
    scpm(ecpTwo, scores, formatting, afterInsert);
  });
  it('should not store objects twice.', function (done) {
    this.timeout(0);
    var insertCount = 0;
    var afterInsert = function (report) {
      console.log(report);
      ++insertCount;
      if (insertCount === 2) {
        scores.count(function (err, count) {
          if (err) throw err;
          assert.equal(count, 3);
          return done();
        });
      }
      scores.count(function (err, count) {
        if (err) throw err;
        assert.equal(count, 3);
        scpm(ecpTwo, scores, formatting, afterInsert);
      });
    };
    scpm(ecpTwo, scores, formatting, afterInsert);
  });
});
