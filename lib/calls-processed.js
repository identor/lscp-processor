var csvparser = require('csv-parse');
var camelCase = require('camel-case');
var fs = require('fs');

const ELS_SCORE = 'ELS Score';
const LS_SCORE = 'LS Score';

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

function parseCsv(csvFilePath, options, callback) {
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

function scoreType(callProcessed) {
  if (callProcessed['elsScorerName'] && callProcessed['elsProcessingTimeSec']) {
    return ELS_SCORE;
  } else if (callProcessed['scorerName'] && callProcessed['processingTimeSec']) {
    return LS_SCORE;
  } else {
    return undefined;
  }
}

module.exports = {
  CallsProcessed: {
    ELS_SCORE: ELS_SCORE,
    LS_SCORE: LS_SCORE,
    scoreType: scoreType,
    parseCsv: parseCsv
  }
};
