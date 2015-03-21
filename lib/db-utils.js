var crypto = require('crypto');
var queries = require('./queries.js');
var CallsProcessed = require('./calls-processed.js').CallsProcessed;

const SAFE_TO_UPSERT = 'Safe upsert';
const DONT_INSERT_DUPLICATE = 'Dont insert, dulicated';
const INSERT_WEIRD_DUPLICATE = 'Weird Duplicate Encountered';
const ERROR = 'error';

function isECPScore(data) {
  return CallsProcessed.scoreType(data) === CallsProcessed.ELS_SCORE;
}

function scoreUpdatability(score, collection, cb) {
  var scores = collection;
  var selector = { _id: score._id };
  scores.findOne(selector, function (err, dbScore) {
    if (err) {
      // give access to the callback for the err variable
      ERROR = err;
      cb(ERROR);
      report.errors.push(err);
    }
    var sameType = !(isECPScore(score) ^ isECPScore(dbScore));
    if (!sameType) {
      return cb(SAFE_TO_UPSERT);
    }
    var dupShasum = crypto.createHash('sha');
    var docShasum = crypto.createHash('sha');
    for (var key in score) {
      if (!dbScore[key]) continue;
      docShasum.update(dbScore[key].toString());
      dupShasum.update(score[key].toString());
    }
    var dupHash = dupShasum.digest('hex');
    var docHash = docShasum.digest('hex');
    var sameShasum = dupHash === docHash;
    if (sameShasum) {
      return cb(DONT_INSERT_DUPLICATE);
    }
    scores.findOne({ _id: dupHash }, function (err, weird) {
      if (err) {
        report.errors.push(err);
        ERROR = err;
        return cb(ERROR);
      }
      if (weird) {
        return cb(DONT_INSERT_DUPLICATE);
      }
      return cb(INSERT_WEIRD_DUPLICATE);
    });
  });
}

function saveToDb(pathToCsv, collection, fieldFormatting, finished) {
  var report = {
    messages: [],
    errors: []
  };
  var executeExitScripts = function() {
    if (finished && typeof(finished) == 'function') finished(report);
  }
  var checkIfUpdatable = function (duplicate, cb) {
    return scoreUpdatability(duplicate, collection, cb);
  };
  /* A safer but slower method of updating the scores. */
  var updateDbRecords = function (index, scores, cb) {
    var selector = { _id: scores[index]._id };
    var opts = { upsert: true };
    var update = { $set: scores[index] };
    var updated = function (err, data) {
      if (err) {
        report.errors.push(err);
        if (err.code === 11000)
          report.errors.push('Record already uploaded...');
      }
      cb();
    };
    collection.update(selector, update, opts, updated);
  };
  var individuallyUpsertScores = function (scores, dupIndex) {
    var upsertedScores = dupIndex;
    for (var i = dupIndex; i < scores.length; i++) {
      updateDbRecords(i, scores, function () {
        upsertedScores++;
        if (upsertedScores === scores.length) {
          report.messages.push(
            'upserted @' + ' ' + (Date.now()-start) / 1000 + ' ' + 'sec/s'
          );
          executeExitScripts();
        }
      });
    }
  };
  var individuallyInsertScores = function (scores, dupIndex) {
    var insertedScores = dupIndex;
    var promptFinished = function() {
      if (insertedScores === scores.length) {
        report.messages.push(
          'inserted @' + ' ' + (Date.now()-start) / 1000 + ' ' + 'sec/s'
        );
        executeExitScripts();
      }
    };
    var insertionFallBack = function (score, status) {
      switch (status) {
        case SAFE_TO_UPSERT:
          updateDbRecords([score], 0, function () {
            insertedScores++;
            promptFinished();
          });
          break;
        case INSERT_WEIRD_DUPLICATE:
          var shasum = crypto.createHash('sha');
          for (var key in score) {
            shasum.update(score[key].toString());
          }
          score['callId'] = score['_id'];
          score['_id'] = shasum.digest('hex');
          collection.insert(score, function (err, data) {
            if (err) report.errors.push(err);
            insertedScores++;
            promptFinished();
          });
          break;
        case DONT_INSERT_DUPLICATE:
        case ERROR:
          insertedScores++;
          report.messages.push(status);
          break;
      }
      promptFinished();
    };
    var updateDbRecords = function (index, scores) {
      var selector = { _id: scores[index]._id };
      var updated = function (err, data) {
        insertedScores++;
        if (err) {
          if (err.code === 11000) {
            insertedScores--;
            report.messages.push(
              'Record already uploaded... inserting to weirdScores.'
            );
            var supInsertionFallBack = function (status) {
              insertionFallBack(scores[index], status);
            };
            checkIfUpdatable(scores[index], supInsertionFallBack);
          } else {
            report.errors.push(err);
          }
        }
        promptFinished();
      };
      collection.insert(scores[index], updated);
    };
    for (var i = dupIndex; i < scores.length; i++) {
      updateDbRecords(i, scores);
    }
  };
  var insertScores = function (scores) {
    scores = scores.filter(function (element) {
      // remove elements without an _id specified
      return !!element._id
    });
    if (scores.length < 1) {
      report.messages.push(
        'Nothing to process...' + ' ' + pathToCsv
      );
      executeExitScripts();
    }
    collection.insert(scores, function (err, data) {
      if (err) {
        var DUP_CODE = 11000
        if (err.code === DUP_CODE) {
          report.messages.push(
            'Duplicate found... executing fallbacks.'
          );
          var duplicate = err.toJSON().op;
          var duplicateIndex = scores.indexOf(duplicate);
          checkIfUpdatable(duplicate, function updateScores(status) {
            if (status === SAFE_TO_UPSERT) {
              report.messages.push('Executing Upsert Fallback.');
              report.messages.push('Upserting scores individually.');
              individuallyUpsertScores(scores, duplicateIndex);
            } else if (status === INSERT_WEIRD_DUPLICATE) {
              report.messages.push('To execute WEIRD DUPLICATION Fallback.');
              report.messages.push('Duplicate callID:' + ' ' + duplicate._id);
              report.messages.push('Inserting scores recheck duplication.');
              individuallyInsertScores(scores, duplicateIndex);
            } else {
              report.messages.push('Duplicate callID:', duplicate._id);
              report.messages.push('These Scores are already uploaded!');
              report.messages.push(
                'exited @' + ' ' + (Date.now()-start) / 1000 + ' ' + 'sec/s'
              );
              executeExitScripts();
            }
          });
        } else {
          report.messages.push('Closing db...' + ' ' + err.message);
          executeExitScripts();
        }
      } else {
        report.messages.push(
          'Successfully inserted scores to db @'
          + ' ' + (Date.now()-start) / 1000 + ' ' + 'sec/s'
        );
        executeExitScripts();
      }
    });
  };
  var start = Date.now();
  var options = {
    mongodb: true,
    fieldFormat: fieldFormatting
  };
  report.messages.push(
    'Processing file:' + ' ' + pathToCsv
  );
  var scores = CallsProcessed.parseCsv(pathToCsv, options, insertScores);
}

module.exports = {
  DbUtils: {
    SAFE_TO_UPSERT: SAFE_TO_UPSERT,
    DONT_INSERT_DUPLICATE: DONT_INSERT_DUPLICATE,
    INSERT_WEIRD_DUPLICATE: INSERT_WEIRD_DUPLICATE,
    ERROR: ERROR,
    scoreUpdatability: scoreUpdatability,
    saveToDb: saveToDb,
    queries: queries
  }
};
