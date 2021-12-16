# Sentinel Archiver
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/iand/sentinel-archiver)

Produces regular archives of on-chain state for the Filecoin network.

Sentinel Archiver is a component of [**Sentinel**](https://github.com/filecoin-project/sentinel), a collection of services which monitor the health and function of the Filecoin network. 


## Overview

Sentinel Archiver works in conjunction with a [Lily](https://github.com/filecoin-project/lily) node to extract data from the Filecoin network and package it into convenient archives for public reuse.

The data is partitioned into separate **tables** defined by the [Lily schema](https://github.com/filecoin-project/lily/tree/master/schemas).
Archive files for each table are produced daily, covering a 24 hour period from midnight UTC. 
Production will be delayed until at least one finality (900 epochs) after the end of the period. 
Each archive file is formatted as CSV compressed using gzip.

Archive files are organised in a directory hierarchy following the pattern `network/format/schema/table/year`

 - Top level is the network name (for example: mainnet)
 - Second level is the format of the data (for example: csv)
 - Third level is the major schema version number being used for the extract (for example: 1)
 - Fourth level is the name of table (for example: messages)
 - Fifth level is the four digit year (for example: 2021)

File names in each directory use following pattern: `table-year-month-day.format.compression`

Example of file in directory hierarchy: `mainnet/csv/1/messages/2021/messages-2021-08-02.csv.gz`


## Notes

The dates for naaming archive files are calculated using UTC and start at midnight.

Archive files files do not contain a header row so multiple CSV files for the same table can simply be concatenated.
A file for each table containing a single header row will be published in the top level schema directory. 
This can be prefixed to any CSV file for that table if needed.
For example: `mainnet/1/messages.header`

A general schema definition for each table will be published as a file containing a name, datatype, nullability and comment for each column/field, for example: `mainnet/1/messages.schema`

JSON is encoded as a string field in the CSV. A null value is represented by the token null (without quotes).

The following tables have json fields:
 - actor_states.state
 - internal_parsed_messages.params
 - parsed_messages.params
 - visor_processing_reports.errors_detected

DateTime fields are formatted in RFC3339 format to millisecond granularity in UTC. For example 2021-08-12T23:20:50.522Z

The following tables have datetime fields:
 - visor_processing_reports.started_at
 - visor_processing_reports.completed_at

Null values are represented by the token `null` without quotes. 


## Code of Conduct

Sentinel Archiver follows the [Filecoin Project Code of Conduct](https://github.com/filecoin-project/community/blob/master/CODE_OF_CONDUCT.md). Before contributing, please acquaint yourself with our social courtesies and expectations.


## Contributing

Welcoming [new issues](https://github.com/iand/sentinel-archiver/issues/new) and [pull requests](https://github.com/iand/sentinel-archiver/pulls).


## License

The Filecoin Project and Sentinel Archiver is dual-licensed under Apache 2.0 and MIT terms:

- Apache License, Version 2.0, ([LICENSE-APACHE](https://github.com/filecoin-project/sentinel-visor/blob/master/LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](https://github.com/filecoin-project/sentinel-visor/blob/master/LICENSE-MIT) or http://opensource.org/licenses/MIT)
