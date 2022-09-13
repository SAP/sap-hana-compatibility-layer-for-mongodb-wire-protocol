[![Test](https://github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/actions/workflows/go-test.yml/badge.svg?branch=main)](https://github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/actions/workflows/go-test.yml)
# SAP Repository Template

Default templates for SAP open source repositories, including LICENSE, .reuse/dep5, Code of Conduct, etc... All repositories on github.com/SAP will be created based on this template.

## To-Do

In case you are the maintainer of a new SAP open source project, these are the steps to do with the template files:

- Check if the default license (Apache 2.0) also applies to your project. A license change should only be required in exceptional cases. If this is the case, please change the [license file](LICENSE).
- Enter the correct metadata for the REUSE tool. See our [wiki page](https://wiki.wdf.sap.corp/wiki/display/ospodocs/Using+the+Reuse+Tool+of+FSFE+for+Copyright+and+License+Information) for details how to do it. You can find an initial .reuse/dep5 file to build on. Please replace the parts inside the single angle quotation marks < > by the specific information for your repository and be sure to run the REUSE tool to validate that the metadata is correct.
- Adjust the contribution guidelines (e.g. add coding style guidelines, pull request checklists, different license if needed etc.)
- Add information about your project to this README (name, description, requirements etc). Especially take care for the <your-project> placeholders - those ones need to be replaced with your project name. See the sections below the horizontal line and [our guidelines on our wiki page](https://wiki.wdf.sap.corp/wiki/display/ospodocs/Guidelines+for+README.md+file) what is required and recommended.
- Remove all content in this README above and including the horizontal line ;)

***

# SAP HANA compatibility layer for MongoDB Wire Protocol

## About this project

SAP HANA compatibility layer for MongoDB Wire Protocol is in the process of becoming a viable drop-in replacement for MongoDB using SAP HANA JSON Document Store as the storage engine. It allows the use of basic CRUD operations with mongosh or any MongoDB driver. SAP HANA compatibility layer for MongoDB Wire Protocol is a fork from FerretDB ([ferretdb.io](https://www.ferretdb.io/)), an open-source alternative to MongoDB. 

## Known differences

- If a field of a document within an array is NULL, it will count as unset when $not is used on the field. This results in the condition of the filter being true instead of false like it would be within MongoDB. 
- When listing the databases with for instance the command "show dbs", the sizes are not the sizes on disk as it would be in MongoDB. Instead it is the size used in memory when the collections of the database are loaded. Any unloaded collection will therefore result in 0 bytes.
- Not all thrown errors are equal to the ones thrown by MongoDB.

## Requirements

- Go 1.18.*
- Go-hdb. A native Go (golang) HANA database driver for Go's sql package. It implements the SAP HANA SQL command network protocol.
- docker (preferably without the need for sudo)
- docker-compose (preferably without the need for sudo)
- GNU make
- A running SAP HANA Cloud instance with SAP HANA JSON Document Store enabled 

For the installation of Go-hdb see the following links:
- [Install the SAP HANA Client](https://developers.sap.com/tutorials/hana-clients-install.html)
- [Connect Using the SAP HANA Go Interface](https://developers.sap.com/tutorials/hana-clients-golang.html)

## Download and Installation

1. Clone the repository and enter the project folder sap-hana-compatibility-layer-for-mongodb-wire-protocol

2. In the project folder sap-hana-compatibility-layer-for-mongodb-wire-protocol run the following:

```
make init
```

It will install all dependencies needed to run SAP HANA compatibility layer for MongoDB Wire Protocol.

3. Open three terminal windows

In terminal window 1 run:
 ```
 docker-compose up
 ```
 
 If sudo is required, use:
 
```
sudo docker-compose up
```
 
 In terminal window 2 run: 
 
```
make run HANAConnectString=<please-insert-connect-string-here>
```

and now in terminal window 3, run the following after making sure the previous two steps started successfully:
```
make mongosh DB=<please-insert-database-name-here>
```
DB is the database name in MongoDB and the schema name in SAP HANA JSON Document Store. If the given database name is not found as a schema in SAP HANA JSON Document Store, a new schema will be created when a collection is created. If no value for DB is given, then it will be set to DB_NAME.

If permission is denied because the rights of sudo are needed, run:
```
make mongosh-sudo DB=<please-insert-databse-name-here>
```

4. Hopefully, all worked out, and you can now run your first MongoDB operations in the shell:

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

To use TLS see: [Setup TLS](SETUP_TLS.md)

## Contributing

This project is open to feature requests/suggestions, bug reports etc. via [GitHub issues](https://github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol/issues). Contribution and feedback are encouraged and always welcome. For more information about how to contribute, the project structure, as well as additional contribution information, see our [Contribution Guidelines](CONTRIBUTING.md).

## Code of Conduct

We as members, contributors, and leaders pledge to make participation in our community a harassment-free experience for everyone. By participating in this project, you agree to abide by its [Code of Conduct](CODE_OF_CONDUCT.md) at all times.

## Licensing

Copyright (2022-)2022 SAP SE or an SAP affiliate company and sap-hana-compatibility-layer-for-mongodb-wire-protocol contributors. Please see our [LICENSE](LICENSE) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol).
