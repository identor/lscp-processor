# LSCP Processor Utils
This module contains utilities for processing lead score files provided by Call Source.

What does this module do?
* Correctly parsing the CSV files provided by Call Source.
* Properly formatting known fields (at the moment) for storage purposes, and
dynamically adding formatting schemes to data when needed.
* Storing the parsed content of the CSV files into a MongoDB datastore.
* Balancing the performance needs (mostly concerned with the data
insert latency) and the quality of the data produced by the application.
* Storing the data from the CSV files in a way where retrieval of appropriate
information is straight forward - Since the time zones of the dates are not
specified, the dates will be stored using an ISO Date String format (ISO 8601)
having a UTC offset of +0000. Rationale: every file contains calls processed
objects in which their processingEndTime (for ls scores) and elsProcessingTime
(for els scores) are in the same day for ex. when processing a file name called
EnhancedLeadScoreCallProcessed20141231.csv the files shall contain call processed
objects having an elsProcessingEnd within December 31, 2014 UTC, after parsing
the csv file.
* Ensuring that every data present in the CSV files are stored in the database.
* Ensuring that there are no duplicate data in the MongoDB database to some
degree (currently using sha-1 hashes to ensure uniqueness of the duplicated callIds).

## The CSV to JavaScript Object parser
Parses specified csv files into a Javascript object.

## Mongodb Storage Utilities
Contains functions which allow the storage of the Javascript objects which are
parsed using the CSV to Javascript object parser. This also contains functions
which ensures the contents of csv files wouldn’t be duplicated in the database.
It also has functions for determining whether a calls processed file is
Enhanced or not, and upserts it to the database if it is. Another feature of
this module includes a mechanism which includes scores which are scored more
than once in a separate database called `weirdScores` to ensure that all data
will be counted as is (while also ensuring the uniqueness of these scores).
