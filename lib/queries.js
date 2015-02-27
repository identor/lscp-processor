function callsProcessedCount(collection, cb) {
  var scores = collection;
  var processAggregatedScores = function (err, data) {
    if (err) sendErrorResponse (err);
    var scorerDataSet = [];
    for (var i = 0; i < data.length; i++) {
      var scorer = data[i]._id.scorer;
      var date = data[i]._id.date;
      var industry = data[i]._id.date;
      var scorerProductivity = {
        scorer: data[i]._id.scorer,
        date: data[i]._id.date,
        industry: data[i]._id.industry,
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
  var aggregateGroup = {
    _id: {
      scorer: '$scorer',
      date: '$fileDate',
      industry: '$industry'
    },
    branch: { $addToSet: '$branch' },
    totalCallDuration: { $sum: '$callDuration' },
    totalProcessingTime: { $sum: '$processingTimeSec' },
    averageCallDuration: { $avg: '$callDuration' },
    averageProcessingTime: { $avg: '$processingTimeSec' },
    callCount: { $sum: 1 }
  };
  var aggregateOpts = [{
    $group: aggregateGroup
  }]
  scores.aggregate(aggregateOpts, processAggregatedScores);
}

module.exports = {
  callsProcessedCount: callsProcessedCount
};
