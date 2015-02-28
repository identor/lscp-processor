var aggregateGroup = {
  _id: {
    scorer: '$scorer',
    date: {
      day: { $dayOfMonth: '$processingEnd' },
      month: { $month: '$processingEnd' },
      year: { $year: '$processingEnd' }
    }
  },
  branch: { $addToSet: '$branch' },
  totalCallDuration: { $sum: '$callDuration' },
  totalProcessingTime: { $sum: '$processingTimeSec' },
  averageCallDuration: { $avg: '$callDuration' },
  averageProcessingTime: { $avg: '$processingTimeSec' },
  callCount: { $sum: 1 }
};

function filteredCallsProcessed(collection, match, cb) {
  var scores = collection;
  var processAggregatedScores = function (err, data) {
    if (err) throw err;
    var scorerDataSet = [];
    for (var i = 0; i < data.length; i++) {
      var scorer = data[i]._id.scorer;
      var date = data[i]._id.date;
      var scorerProductivity = {
        scorer: data[i]._id.scorer,
        date: data[i]._id.date,
        totalCallDuration: data[i].totalCallDuration,
        totalProcessingTime: data[i].totalProcessingTime,
        averageCallDuration: data[i].averageCallDuration,
        averageProcessingTime: data[i].averageProcessingTime,
        callCount: data[i].callCount
      };
      scorerDataSet.push(scorerProductivity);
    }
    cb(scorerDataSet);
  };
  var grouping = { $group: aggregateGroup };
  if (match) {
    var matching = { $match: match };
    var aggregateOpts = [matching, grouping];
  } else {
    var aggregateOpts = [grouping];
  }
  scores.aggregate(aggregateOpts, processAggregatedScores);
}

function callsProcessed(collection, dateFrom, dateTo, cb) {
  var cb = arguments[arguments.length-1];
  if (typeof dateFrom === 'function') {
    dateFrom = null;
  }
  if (typeof dateTo === 'function') {
    dateTo = null;
  }
  if (dateFrom) {
    var match = {};
    match.processingEnd = {};
    match.processingEnd.$gte = dateFrom;
  }
  if (dateTo) {
    var match = {};
    match.processingEnd = match.processingEnd || {};
    match.processingEnd.$lt = dateTo;
  }
  filteredCallsProcessed(collection, match, cb);
}

function scorerCallsProcessed(collection, scorer, dateFrom, dateTo, cb) {
  var cb = arguments[arguments.length-1];
  if (typeof dateFrom === 'function') {
    dateFrom = null;
  }
  var match = { scorer: scorer };
  if (dateFrom) {
    match.processingEnd = {};
    match.processingEnd.$gte = dateFrom;
  }
  if (dateTo) {
    match.processingEnd = match.processingEnd || {};
    match.processingEnd.$lt = dateTo;
  }
  filteredCallsProcessed(collection, match, cb);
}

module.exports = {
  callsProcessed: callsProcessed,
  scorerCallsProcessed: scorerCallsProcessed
};
