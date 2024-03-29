[![Test](https://github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/actions/workflows/go-test.yml/badge.svg?branch=main)](https://github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/actions/workflows/go-test.yml)
[![REUSE status](https://api.reuse.software/badge/github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol)](https://api.reuse.software/info/github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol)

# SAP HANA compatibility layer for MongoDB Wire Protocol

## About this project

SAP HANA compatibility layer for MongoDB Wire Protocol is in the process of becoming a viable drop-in replacement for MongoDB using SAP HANA JSON Document Store as the storage engine. It allows the use of basic CRUD operations with mongosh or any MongoDB driver. SAP HANA compatibility layer for MongoDB Wire Protocol is a fork from FerretDB ([ferretdb.io](https://www.ferretdb.io/)), an open-source alternative to MongoDB. Please see the following for an overview of [supported MongoDB commands](SUPPORTED_MONGODB_COMMANDS.md#supported-mongodb-commands) and [supported datatypes](SUPPORTED_MONGODB_COMMANDS.md#supported-datatypes).

This project is a working prototype that comes without warranty. It is not recommended nor intended to be used productively.

## Features
- Can be used with the MongoDB shell `mongosh` or any MongoDB driver
- `TLS` is supported
- Supports basic collection and database commands
- Supports basic `CRUD` operations 
- Numerous query operators, cursor methods and bulk operations can be used with the supported `CRUD`operations

## Known differences

- If a field of a document within an array is `NULL`, it will count as unset when `$not` is used on the field. This results in the condition of the filter being `true` instead of `false` like it would be within MongoDB. This is the case when for instance using `$elemMatch`. 
- When listing the databases with for instance the command `show dbs`, the sizes are not the sizes on disk as it would be in MongoDB. Instead it is the size used in memory when the collections of the database are loaded. Any unloaded collection will therefore result in 0 bytes.
- Not all thrown errors are equal to the ones thrown by MongoDB.
- Collections and databases are case insensitive and are all uppercase letters. Furthermore, `TEST` cannot be used as a name for a database.

If further differences are found, please report this to [a project maintainer](.reuse/dep5).

## Requirements

- Linux
- Go 1.18.*
- Go-hdb. A native Go (golang) HANA database driver for Go's sql package. It implements the SAP HANA SQL command network protocol.
- docker (preferably without the need for `sudo`)
- docker-compose (preferably without the need for `sudo`)
- GNU make
- A running SAP HANA Cloud instance with SAP HANA JSON Document Store enabled 

For the installation of Go-hdb see the following links:
- [Install the SAP HANA Client](https://developers.sap.com/tutorials/hana-clients-install.html)
- [Connect Using the SAP HANA Go Interface](https://developers.sap.com/tutorials/hana-clients-golang.html)

Please note it will not work with the go-hdb found here: https://github.com/SAP/go-hdb. It must be the official Go driver used in the above mentioned links.

## Quick Setup

1. Clone the repository and enter the project folder `sap-hana-compatibility-layer-for-mongodb-wire-protocol`

2. In the project folder `sap-hana-compatibility-layer-for-mongodb-wire-protocol` run the following:

```
make init
```

It will install all dependencies needed to run SAP HANA compatibility layer for MongoDB Wire Protocol.

3. Open three terminal windows

In terminal window 1 run:
 ```
 docker-compose up
 ```
 
 If `sudo` is required, use:
 
```
sudo docker-compose up
```
 
 In terminal window 2 run: 
 
```
make run HANAConnectString=<please-insert-connect-string-here>
```
Depending on the shell used, it might be necessary to put the connect string between quotation marks "". An exmaple of a connection string is: `hdb://User1:Password1@999deec0-ccb7-4a5e-b317-d419e19be648.hana.prod-us10.hanacloud.ondemand.com:443`.

and now in terminal window 3, run the following after making sure the previous two steps started successfully:
```
make mongosh DB=<please-insert-database-name-here>
```
`DB` is the database name in MongoDB and the schema name in SAP HANA JSON Document Store. If the given database name is not found as a schema in SAP HANA JSON Document Store, a new schema will be created when a collection is created. If no value for DB is given, then it will be set to `DB_NAME`.

If permission is denied because the rights of `sudo` are needed, run:
```
make mongosh-sudo DB=<please-insert-databse-name-here>
```

4. Hopefully, all worked out, and you can now run your first MongoDB operations in `mongosh`:

```
db.createCollection("firstCollection")
```

```
db.firstCollection.insertOne({we: "did", it: "!"})
```

```
db.firstCollection.find()
```

## TLS

To use TLS see: [Setup TLS](SETUP_TLS.md#setup-tls)

## Contributing

This project is open to feature requests/suggestions, bug reports etc. via [GitHub issues](https://github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/issues). Contribution and feedback are encouraged and always welcome. For more information about how to contribute, the project structure, as well as additional contribution information, see our [Contribution Guidelines](CONTRIBUTING.md#contributing).

## Code of Conduct

We as members, contributors, and leaders pledge to make participation in our community a harassment-free experience for everyone. By participating in this project, you agree to abide by its [Code of Conduct](CODE_OF_CONDUCT.md#Contributor-Covenant-Code-of-Conduct) at all times.

## Licensing

Copyright (2022-)2022 SAP SE or an SAP affiliate company and sap-hana-compatibility-layer-for-mongodb-wire-protocol contributors. Please see our [LICENSE](LICENSE) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol).
