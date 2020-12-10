# Batch import for Laravel's eloquent models
[![Latest Version on Packagist](https://img.shields.io/packagist/v/mehr-it/lara-db-batch-import.svg?style=flat-square)](https://packagist.org/packages/mehr-it/lara-db-batch-import)
[![Build Status](https://travis-ci.org/mehr-it/lara-db-batch-import.svg?branch=master)](https://travis-ci.org/mehr-it/lara-db-batch-import)

Batch imports are a common task for any larger system. This packages implements a batch import 
for laravel's eloquent models.

## How to use
First the `ProvidesBatchImport` and the `DbExtensions` (from package [mehr-it/lara-db-ext](https://packagist.org/packages/mehr-it/lara-db-ext)) trait must be added to a model:

    class User extends Model {
        uses DbExtensions;
        uses ProvidesBatchImport;
    } 
    
This adds a new static method called `batchImport()` to the model. It creates a new batch import 
instance which can be configured and used to import data. See following example:

    User::batchImport()
        ->matchBy('name')
        ->updateIfExists(['phone', 'email'])
        ->import([
            new User([
                'name' => 'Hans Maier',
                'phone' = '+ 49 6081 1234',
                'email' = 'hans.maier@example.com',
            ]),
            new User([
                'name' => 'Max Mustermann',
                'phone' = '+ 49 61200 1234',
                'email' = 'max.m@example.com',
            ]),
            /* ... */
        ]);
        
The example code intents to import user data. New records are insert for existing records
(determined by matching "name" field) the "phone" and "email" fields are updated.

Behind the scenes, the import data is processed in chunks of 500 records: For each chunk, the
database is searched for existing records. In the given example, the users table is queried for
records with a name matching any of the given names within the current chunk. The result is
compared to the chunk data: Existing records are checked for required updates and new records
are marked to insert. The resulting modifications are sent to the database using bulk insert/update
strategies.


### Record matching conditions
The fields used to consider two records as "matching" (and therefore to update instead of insert new 
data) can be specified using the `matchBy()` method. Multiple fields can be passed as array.

Records are considered to match if the string representation of all given matchBy fields is equal.

**Attention:** The according to SQL's "three-valued logic" comparing `null` values never returns `true`.
This means, that **a record won't match any other record if any of the "matchBy" fields is null!**

The comparison is limited to equal comparison, however a callable can be passed to process the value
before comparison:

    $import->matchBy('name' => function($v) { return strtolower($v); });
    
Without any explicitly setting "matchBy" fields, the model's primary key is used.
    
    
### Updating existing records
If existing records should be updated, the `updateIfExists()` method can be used to specify a list
of fields to update in case of existence.

If `updateIfExists` is not invoked, only new records are insert!

The `updateIfExists()` method also accepts updates with a static value, an SQL expression or a
callable to generate a more customized value:

    $import->updateIfExists([
        'phone' => function($v) {
            return str_replace(' ', '', $v);       
        },
        'email' => new Expression('lower(email)'),
    ]);
    
    
### Adding callbacks
Often it is required to know which records have been affected by the batch import. The `onInserted()`,
`onUpdated()` and `onInsertedOrUpdated()` methods can be used to register callbacks to receive the corresponding records 
(in chunks of 500 per default).

    $import->onUpdated(function($records) {
        foreach($record as $currRecord) {
            /* do s.th. */
        }
    });
    
Note: The model instances passed to the callbacks are not inevitably the same as the ones passed to
the import function. Further they do not contain updated timestamps or inserted id values, because
mass insert/update strategies are used for database operations.


### Determining missing records
Determining which records have been updated or inserted is quite easy because all these records are
"seen" by the bulk import algorithm. But the bulk import is unaware of any other records.

Nevertheless, it can help to detect them afterwards. It can mark any "seen" records with a sequential
batch id. This makes it possible to query for "missing" records in a second step. There a new batch
id must be passed to the `withBatchId()` method:

    $import->withBatchId(100001);
    
This will set the "last_batch_id" (which must exist in the database) to the given value. 
    
**It is important to increment the batch id for each new batch import!**

After the import, any missing records can be queried using the `whereMissingAfterBatch()`
condition:

    User::query()
        ->whereMissingAfterBatch(100001)
        ->chunk(500, function($records) {
            /* process missing records */
        });
        
It returns any records where the batch id is null or less than the given value.
        
For performant SQL operations, corresponding indices should be set for the tables. 


#### Models with batch id
Following best practices, the batch id should be generated per model. Therefor it is a good
idea to add the generation logic to the model class. Implementing the `GeneratesBatchIds`
interface lets batch import operations automatically fetch the next batch id from the model
without manually invoking `withBatchId()`:

    class User extends Model implements GeneratesBatchIds {
        
        /**
         * Gets the next batch id
         * @return string The next batch id as string
         */
        public function nextBatchId(): string {
            
            /* custom logic here */
            
        }
    
    }
    
The `import()` method accepts a second parameter which will return the last used batch id.
Alternatively, the `getLastBatchId()` method can be used:

    $import->import($data, $lastBatchId);
    
    // or
    
    $import->getLastBatchId();
    
    

    
### Prepared imports
Sometimes it can be very handy to prepare an import, then collect the data and flush the import afterwards.
The `prepare()` method can be used for this:

    // prepare
    $prepared = $import->prepare();
    
    // add records
    $prepared->add($record1);
    $prepared->addMultiple([$record2, $record3]);
    
    // flush
    $prepared->flush($lastBatchId);
 
 
### Bypassing models
Using models for managing database data offers a clean and comfortable interface. However, this
comes with the drawback of some overhead. When performing bulk imports with large datasets
model attribute set/get operations are performed a thousand times. The performance impact can
be significant. 

**If you do not need model attribute functionality**, such as mutators, accessors, casts and so on
the `bypassModel()` method can make your application much faster.

    $import->bypassModel();
    
In such case you must provide the data as raw arrays instead of model instances. 

The `bypassModel()` method also accepts a second parameter called "rawComparators". It accepts 
custom comparator functions for fields. These are used to compare the new data with existing
data to detect changes. Eg. a decimal stored in database might be returned as '12.90' but your
input might be '12.9'. Without a custom comparator the row will be detected as change. But a 
custom comparator can avoid this:

    $import->bypassModel(true, [
        'price' => function($new, $old) {
            return bccomp($new, $old, 2);
        }
    ]);