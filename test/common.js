var MongoClient = require('mongodb').MongoClient;

var defs = {};

defs.ecpOne = 'test/ECPwiththirtyelements.csv';
defs.ecpTwo = 'test/ECPwithconflict.csv';
defs.cpOne = 'test/CPone.csv';
defs.scores;

defs.formatting = [
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
];

defs.initDb = function (done) {
  var droppedDb = false;
  var connectedToDb = false;
  var isProcessingFinished = function () {
    if (droppedDb && connectedToDb) {
      done();
    }
  };
  MongoClient.connect('mongodb://localhost/test', function (err, db) {
    if (err) throw err;
    connectedToDb = true;
    defs.mongodb = db;
    defs.scores = defs.mongodb.collection('scores');
    defs.mongodb.dropDatabase(function (err, result) {
      droppedDb = true;
      isProcessingFinished();
    });
    isProcessingFinished();
  });
};

module.exports = defs;
