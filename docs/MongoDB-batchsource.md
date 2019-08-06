# MongoDB Batch Source


Description
-----------
Reads documents from a MongoDB collection and converts each document into a StructuredRecord with the help
of a specified schema. The user can optionally provide input query.


Configuration
-------------
**Reference Name:** Name used to uniquely identify this source for lineage, annotating metadata, etc.

**Host:** Host that MongoDB is running on.

**Port:** Port that MongoDB is listening to.

**Database:** MongoDB database name.

**Collection:** Name of the database collection to read from.

**Output Schema:** Specifies the schema of the documents.

**Input Query:** Optionally filter the input collection with a query. This query must be represented in JSON format
and use the [MongoDB extended JSON format] to represent non-native JSON data types. (Macro-enabled)

**Username:** User identity for connecting to the specified database.

**Password:** Password to use to connect to the specified database.

**On Record Error:** Specifies how to handle error in record processing. An error will be thrown if failed to parse
value according to a provided schema.

**Authentication Connection String:** Auxiliary MongoDB connection string to authenticate against when constructing
splits. (Macro-enabled)

**Connection Arguments:** A list of arbitrary string key/value pairs as connection arguments. See
[Connection String Options] for a full description of these arguments.

[MongoDB extended JSON format]:
http://docs.mongodb.org/manual/reference/mongodb-extended-json/

[Connection String Options]:
https://docs.mongodb.com/manual/reference/connection-string/#connections-connection-options
