# Command Examples

This file contains some examples for commands you can execute as a way to get started using SAP HANA compatibility layer for MongoDB Wire Protocol.

### Insert documents
```
db.FURNITURE.insertMany([{type: "dinner table", category: "Kitchen", measurements: {width: 120, depth: 60, hight: 75}, color: ["brown", "black", "white"]},
  {type: "bed", category: "bedroom", measurements: {width: 165, depth: 200, hight: 55}, color: ["grey", "black", "white"]},
  {type: "bar chair", category: "Kitchen", measurements: {width: 50, depth: 45, hight: 120}, color: ["red", "green"]},
  {type: "bedroom closet", category: "bedroom", measurements: {width: 150, depth: 55, hight: 200}, color: ["brown"]}]);
```

### Retrive all documents
```
db.FURNITURE.find()
```

### Update a document
```
db.FURNITURE.updateOne({type: "bedroom closet"}, {$set: {"measurements.width": 200}})
```

### Filter documents
```
db.FURNITURE.find({category: "bedroom", measurements: {width: 200, depth: 55, hight: 200}})
```
