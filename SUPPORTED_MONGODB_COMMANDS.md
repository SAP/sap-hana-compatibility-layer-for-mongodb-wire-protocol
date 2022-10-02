# Supported MongoDB commands

Here you will find all MongoDB commands supported by SAP HANA compatibility layer for MongoDB Wire Protocol listed. Any command not listed here can be considered 
unsupported and will return an error. The commands will be written as mongosh methods.

## Collection commands
* `db.createCollection(name, options)`
  * `name` is supported and is case insensitive. The created collection will be all uppercase letters.
  * `options` are not supported.
* `db.collection.drop(options)`
  * `options` are not supported. Only `db.collection.drop()` is supported.
* `show collections`

## Database commands
* `use <DATABASE_NAME>`
  * If the given database does not exist, it will first be created as a schema in SAP HANA JSON Document Store when `show dbs`, `db.createCollection()`, 
  `db.collection.insertOne` or `db.collection.insertMany()` is executed. 
  * It is case insensitive and all databases are all uppercase letters.
  * If the database exists it will behave like MongoDB.
* `db.dropDatabase()`
  * This will delete the schema in SAP HANA JSON Document Store with the same name as the database.  
* `show dbs`
  * The size of each database is calculated by adding the sizes of all loaded collections of a database. Any collection not in memory will not be a part of the size given
  for a database. This behavior differs from the behavior of MongoDB.
  
## CRUD operations
* `db.collection.find(query, projection, options)`
  * `query`
    *  Can filter all [supported datatypes](#supported-datatypes). Not supported is filtering an index of an array within an array, i.e. `"array.2.3": "value"`,
    and it is also not supported to filter a field based on an array, i.e. `field: [1, 2]` is not possible.
    * Following query operators are supported:
      * `$eq` 
      * `$gt`, `$gte`
      * `$lt`, `$lte`
      * `$ne`
      * `$and`
      * `$not`
      * `$not`
      * `$or`
      * `$exists`
      * `$regex`
        * Does not support regex options or the regex operators `(?i)` and `(?-i)`.
      * `$all`
      * `$elemMatch` - see [known differences](https://github.com/SAP/sap-hana-compatibility-layer-for-mongodb-wire-protocol#known-differences)
      * `$size`
  * `projection`
    * Supports `inclusion` and `exclusion`.
    * `inclusion`
      * Does not support projection on nested objects.  
  * `options`
    * Supports limit and basic sort. 
* `db.collection.insertOne(document, writeConcern)` 
  * `document` can contain any of the [supported datatypes](#supported-datatypes).
  * `writeConcern` is not supported.
* `db.collection.insertMany(documents, writeConcern, ordered)`
  * `documents` can contain any of the [supported datatypes](#supported-datatypes).
  * `writeConcern` is not supported.
  * `ordered` is not supported.
* `db.collection.updateOne(filter, update, options)` and `db.collection.updateMany(filter, update, options)`
  * `filter` supports the same as what is mentioned for `query` for `db.collection.find()`
  * `update` can be used with `$set` and `$unset`.
    * `$set` cannot be used to set a field equal to an array.
  * `options` are not supported.
* `db.collection.deleteOne(filter, options)` and `db.collection.deleteMany(filter, options)`
  *  `filter` supports the same as what is mentioned for `query` for `db.collection.find()`
  * `options` are not supported.

## Cursor methods
* `cursor.count()`
* `cursor.sort()`
* `cursor.limit()`
  * Does not support values less than 0.

## Bulk operations
* `db.collection.bulkWrite(operations, writeConcern, ordered)`
  * `operations` can be any of the supported operations mentioned in this document.
  * `writeConcern` is not supported.
  * `ordered` is not supported.


# Supported datatypes
* String
* Object
* Array
* ObjectId
* Boolean
* Null
* Regular Expression (only for filter)
* 32-bit integer
* 64-bit integer

