# Sentinel Archiver
[![go.dev reference](https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/filecoin-project/sentinel-archiver)

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

## Outline of Operation

Sentinel Archiver needs to be paired with a Lily node which it will use to extract data by running walk jobs.
It produces archive files by executing walks against the Lily node, verifying the files produced do not contain errors and then compressing and shipping to the final archive location.

The `run` command starts the archiver as a long running daemon that will attempt to maintain a complete archive of the Filecoin chain.

On startup the archiver scans the files that have been shipped so far to determine whether there are any missing archive files that need to be reprocessed.
If it finds one or more missing files for a day it prepares a walk with the appropriate tasks and height range, submits it to Lily and waits for the walk to complete.
If all files are present the archiver will wait until it is allowed to process the current day's data. 
The earliest this may happen is one finality (900 epochs) after midnight (which is about 7:30AM).

## Running

The `run` command accepts several flags that may be used to configure the behaviour of the archiver.

 - `--ship-path` must be set to the root directory where the final archive files will be written. The archiver will create the necessary file hierachy beneath this directory (i.e. `<ship path>/network/format/schema/table/year`)
 - `--storage-name` must be set to the name of a file storage defined in the [Lily config file](https://lilium.sh/lily/setup.html#storage-definitions). If the section in the config file is `[Storage.File.CSV]` then the name will be `CSV`.
 - `--storage-path` must be set to the directory where Lily writes its output files. This is the path assigned to the named file storage in the [Lily config file](https://lilium.sh/lily/setup.html#storage-definitions).
 - `--tasks` may optionally be set to limit the tasks that this instance is responsible for. By default all known tasks will be run. Responsibility for different tasks may be split between multiple instances of the archiver by specifying a different subset of tasks for each one.
 - `--min-height` may be used to instruct the archiver to only consider archives after a certain epoch. This can be used to operate against a Lily node that only contains a partial history of the network, such as one initialised from a car export.

By default the archiver assumes it is operating against mainnet. The following flags may be used to configure it to operate against an alternate network. Note that these flags are hidden from the help output since they are rarely needed.
It is crucial that the Lily node paired with the archiver must have been built specifically for the selected network. Consult the [lily documentation](https://lilium.sh/lily/setup.html#build) for instructions on how to do this. 

 - `--network` must be set to the name of the network. This is only used to determine the name of the directory in which shipped files should be placed.
 - `--genesis-ts` must be set to the UNIX timestamp of the genesis of the alternate network. This may vary depending on when the network was created. 

If Lily is restarted or becomes unavailable during a walk, the archiver will wait until it is back online and resubmit the walk.

The archiver may be also restarted while a walk is in progress and it will attempt to find the correct one to wait for when it starts.

Exports that contain errors are not shipped, leaving a potential gap in the archive. When the archiver next scans the archive folder these missing files will automatically be scheduled for processing. The archiver will issue a new walk to cover just the failed tables. (Note: although this prevents the archiver from shipping bad exports it can also hold up all exports if the errors encountered are permanent failures since they will appear during any subsequent walk).

## Notes

The dates for naming archive files are calculated using UTC and start at midnight.

Archive files files do not contain a header row so multiple CSV files for the same table can simply be concatenated.
A file for each table containing a single header row will be added to each table’s folder.

Header files providing column names for each table are in each table’s folder.
This can be prefixed to any CSV file for that table if needed.
For example: `mainnet/csv/1/messages/messages.header`

A general schema definition for each table will be published in each table’s folder. 
This uses postgresql compatible DDL to document the table's column names and expected types. 
For example: `mainnet/csv/1/messages.schema`

JSON is encoded as a string field in the CSV. A null value is represented by the token `null` (without quotes).

The following tables have json fields:
 - actor_states.state
 - internal_parsed_messages.params
 - parsed_messages.params
 - visor_processing_reports.errors_detected

DateTime fields are formatted in RFC3339 format to millisecond granularity in UTC. For example 2021-08-12T23:20:50.522Z

The following tables have datetime fields:
 - visor_processing_reports.started_at
 - visor_processing_reports.completed_at

Refer to the [Lily project](https://github.com/filecoin-project/lily) for precise details of the CSV export.

## Code of Conduct

Sentinel Archiver follows the [Filecoin Project Code of Conduct](https://github.com/filecoin-project/community/blob/master/CODE_OF_CONDUCT.md). Before contributing, please acquaint yourself with our social courtesies and expectations.


## Contributing

Welcoming [new issues](https://github.com/filecoin-project/sentinel-archiver/issues/new) and [pull requests](https://github.com/filecoin-project/sentinel-archiver/pulls).


## License

The Filecoin Project and Sentinel Archiver is dual-licensed under Apache 2.0 and MIT terms:

- Apache License, Version 2.0, ([LICENSE-APACHE](https://github.com/filecoin-project/sentinel-visor/blob/master/LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](https://github.com/filecoin-project/sentinel-visor/blob/master/LICENSE-MIT) or http://opensource.org/licenses/MIT)
