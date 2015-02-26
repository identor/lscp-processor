var csvparser = require('csv-parse');
var fs = require('fs');
var camelCase = require('camel-case');
var crypto = require('crypto');

function formatFields(fieldFormat, data) {
  for (var i in fieldFormat) {
    var field = fieldFormat[i].name;
    if (!data[field]) continue;
    switch(fieldFormat[i].type) {
      case 'Number':
        if (!data[field]) break;
        data[field] = +data[field];
        break;
      case 'Date':
        if (!data[field]) break;
        var zone = fieldFormat[i].zone;
        data[field] = new Date(data[field] + zone);
        break;
    }
  }
}

function createCallsProcessedObject(csvFilePath, options, callback) {
  var opts = options || {};
  var callback = arguments[arguments.length-1];
  var start;
  var parser = csvparser({ columns: true, relax: true });
  var result = [];
  start = Date.now();
  var readStream = fs.createReadStream(csvFilePath).pipe(parser);
  parser.on('data', function (data) {
    for (var key in data) {
      /* Note: Always a string... if empty ignore, keep the database clean
       * of empty attributes.
       */
      if (!!data[key]) {
        data[camelCase(key)] = data[key];
      }
      delete data[key];
    }
    if (data.callId && opts.mongodb) {
      data._id = data.callId;
      delete data.callId;
    }
    if (opts.fieldFormat) {
      formatFields(opts.fieldFormat, data);
    }
    result.push(data);
  });
  parser.on('end', function () {
    console.log('finished creating callsprocessed records @',
                (Date.now()-start) / 1000, 'sec/s');
    console.log('data size:', result.length);
    callback(result);
  });
}

function isECPScore(data) {
  return !!data['elsScorerName'];
}

function storeCallsProcessedToMongo(pathToCsv, mongodb, finished) {
  var db = mongodb;
  var executeExitScripts = function() {
    if (finished && typeof(finished) == 'function') finished.call();
  }
  var SAFE_TO_UPSERT = 'Safe upsert';
  var DONT_INSERT_DUPLICATE = 'Dont insert, dulicated';
  var INSERT_WEIRD_DUPLICATE = 'Weird Duplicate Encountered';
  var ERROR = 'error';
  var checkIfUpdatable = function (duplicate, cb) {
    var scores = db.collection('scores');
    var selector = { _id: duplicate._id };
    scores.findOne(selector, function (err, score) {
      if (err) {
        // give access to the callback for the err variable
        ERROR = err;
        cb(ERROR);
        console.log(err);
      }
      var sameType = isECPScore(score) && isECPScore(duplicate);
      if (!sameType) {
        return cb(SAFE_UPSERT);
      }
      var dupShasum = crypto.createHash('sha');
      var docShasum = crypto.createHash('sha');
      for (var key in duplicate) {
        docShasum.update(score[key].toString());
        dupShasum.update(duplicate[key].toString());
      }
      var dupHash = dupShasum.digest('hex');
      var docHash = docShasum.digest('hex');
      var sameShasum = dupHash === docHash;
      if (sameShasum) {
        return cb(DONT_INSERT_DUPLICATE);
      }
      scores.findOne({ _id: dupHash }, function (err, weird) {
        if (err) {
          console.log(err);
          ERROR = err;
          return cb(ERROR);
        }
        if (weird) {
          return cb(DONT_INSERT_DUPLICATE);
        }
        return cb(INSERT_WEIRD_DUPLICATE);
      });
    });
  };
  /* A safer but slower method of updating the scores. */
  var updateDbRecords = function (index, scores, cb) {
    var selector = { _id: scores[index]._id };
    var opts = { upsert: true };
    var update = { $set: scores[index] };
    var updated = function (err, data) {
      if (err) {
        console.log(err.message);
        if (err.code === 11000) console.log('Record already uploaded...');
      }
      cb();
    };
    db.collection('scores').update(selector, update, opts, updated);
  };
  var individuallyUpsertScores = function (scores, dupIndex) {
    var upsertedScores = dupIndex;
    for (var i = dupIndex; i < scores.length; i++) {
      updateDbRecords(i, scores, function () {
        upsertedScores++;
        if (upsertedScores === scores.length) {
          console.log('upserted @', (Date.now()-start) / 1000, 'sec/s');
          executeExitScripts();
        }
      });
    }
  };
  var individuallyInsertScores = function (scores, dupIndex) {
    var insertedScores = dupIndex;
    var promptFinished = function() {
      if (insertedScores === scores.length) {
        console.log('inserted @', (Date.now()-start) / 1000, 'sec/s');
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
          db.collection('scores').insert(score, function (err, data) {
            if (err) console.log(err);
            insertedScores++;
            promptFinished();
          });
          break;
        case DONT_INSERT_DUPLICATE:
        case ERROR:
          insertedScores++;
          console.log(status);
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
            console.log('Record already uploaded... inserting to weirdScores.');
            var supInsertionFallBack = function (status) {
              insertionFallBack(scores[index], status);
            };
            checkIfUpdatable(scores[index], supInsertionFallBack);
          } else {
            console.log(err);
          }
        }
        promptFinished();
      };
      db.collection('scores').insert(scores[index], updated);
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
      console.log('Nothing to process...', pathToCsv);
      executeExitScripts();
    }
    db.collection('scores').insert(scores, function (err, data) {
      if (err) {
        var DUP_CODE = 11000
        if (err.code === DUP_CODE) {
          console.log('Duplicate found... executing fallbacks.');
          var duplicate = err.toJSON().op;
          var duplicateIndex = scores.indexOf(duplicate);
          checkIfUpdatable(duplicate, function updateScores(status) {
            if (status === SAFE_TO_UPSERT) {
              console.log('Executing Upsert Fallback.');
              console.log('Upserting scores individually.');
              individuallyUpsertScores(scores, duplicateIndex);
            } else if (status === INSERT_WEIRD_DUPLICATE) {
              console.log('To execute WEIRD DUPLICATION Fallback.');
              console.log('Duplicate callID:', duplicate._id);
              console.log('Inserting scores recheck duplication.');
              individuallyInsertScores(scores, duplicateIndex);
            } else {
              console.log('Duplicate callID:', duplicate._id);
              console.log('These Scores are already uploaded!');
              console.log('exited @', (Date.now()-start) / 1000, 'sec/s');
              executeExitScripts();
            }
          });
        } else {
          console.log('Closing db...', err.message);
          executeExitScripts();
        }
      } else {
        console.log('Successfully inserted scores to db @',
                    (Date.now()-start) / 1000, 'sec/s');
        executeExitScripts();
      }
    });
  };
  var start = Date.now();
  var options = {
    mongodb: true,
    fieldFormat: [
      { name: 'processingStart', type: 'Date', zone: '+0000' },
      { name: 'processingEnd', type: 'Date', zone: '+0000' },
      { name: 'callStartTime', type: 'Date', zone: '+0000' },
      { name: 'elsProcessingStart', type: 'Date', zone: '+0000' },
      { name: 'elsProcessingEnd', type: 'Date', zone: '+0000' },
      { name: 'elsProcessingEnd', type: 'Date', zone: '+0000' },
      { name: 'processingTimeSec', type: 'Number' },
      { name: 'elsProcessingTimeSec', type: 'Number' },
      { name: 'accountId', type: 'Number' },
      { name: 'index', type: 'Number' },
      { name: 'callDuration', type: 'Number' },
      { name: '_id', type: 'Number' },
    ]
  };
  console.log('Processing file:', pathToCsv);
  var scores = createCallsProcessedObject(pathToCsv, options, insertScores);
}

module.exports = {
  createCallsProcessedObject: createCallsProcessedObject,
  storeCallsProcessedToMongo: storeCallsProcessedToMongo
};
