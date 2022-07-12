# SAP HANA compatibility layer for MongoDB Wire Protocol

SAP HANA compatibility layer for MongoDB Wire Protocol is a fork from FerretDB ([ferretdb.io](url)), an open-source alternative to MongoDB. SAP HANA compatibility layer is in the process of becoming a viable drop-in replacement for MongoDB. It works as a stateless proxy, converting MongoDB wire protocol queries to SQL. The SQL is then sent to SAP HANA JSON Document Store, the database engine of SAP HANA compatibility layer. MongoDB drivers and shell will, when connected to SAP HANA compatibility layer, behave as if it was connected to a MongoDB instance when in reality, everything is stored on and retrieved from SAP HANA JSON Document Store.  
Perfect for companies looking to change from MongoDB to SAP HANA JSON Document Store. 

## Scope

SAP HANA compatibility layer for MongoDB Wire Protocol will be compatible with MongoDB drivers and shell. The first version will implement the basic MongoDB CRUD operations and support all datatypes supported in SAP HANA JSON Document Store.


## Current state

Prototype. Missing tests. 


## Quickstart

These steps describe a quick local setup on linux.

1. Make sure you have the following installed:
- Go 1.18.*
- Go-hdb. A native Go (golang) HANA database driver for Go's sql package. It implements the SAP HANA SQL command network protocol.
- docker (preferably without the need for sudo)
- docker-compose (preferably without the need for sudo)
- make

Furthermore, a running HANA instance with SAP HANA JSON Document Store enabled is necessary.

For the installation of Go-hdb see the following links:
- [https://developers.sap.com/tutorials/hana-clients-install.html](url)
- [https://developers.sap.com/tutorials/hana-clients-golang.html](url)

2. Clone the repository

3. After cloning, enter the folder sap-hana-compatibility-layer-for-mongodb-wire-protocol and open the file /cmd/SAPHANACompatibilityLayer/main.go. Then insert a connect string for a HANA Cloud instance on line 47. Do the same in file /internal/hana/hanapool.go on line 51.

4. In the folder sap-hana-compatibility-layer-for-mongodb-wire-protocol run the following:

```
make init
```

It will download all modules needed for running SAP HANA compatibility layer for MongoDB Wire Protocol

5. Open three terminal windows

In terminal window 1 run:
 ```
 docker-compose up
 ```
 
 If sudo required, use:
 
```
sudo docker-compose up
```
 
 In terminal window 2 run: 
 
```
make run
```

and now in terminal window 3 run:
```
make mongosh
```

If permission is denied because of lack of sudo, run:
```
make mongosh-sudo
```

6. Hopefully, all worked out, and you can now run your first MongoDB operations in the shell:

```
db.createCollection("firstCollection")
```

```
db.firstCollection.insertOne({we: "did", it: "!"})
```

```
db.firstCollection.find()
```
