<?php


	namespace MehrIt\LaraDbBatchImport;


	use Carbon\Carbon;
	use Illuminate\Database\Eloquent\Builder;
	use Illuminate\Database\Eloquent\Model;
	use Illuminate\Support\Arr;
	use InvalidArgumentException;
	use MehrIt\Buffer\FlushingBuffer;
	use MehrIt\LaraDbBatchImport\Concerns\BatchImportInfo;
	use MehrIt\LaraTransactions\TransactionManager;
	use RuntimeException;
	use Throwable;

	class BatchImport
	{
		use BatchImportInfo;

		protected $bufferSize = 500;

		protected $callbackBufferSize = 500;

		protected $matchBy;

		protected $model;

		protected $updateFields = [];

		protected $updateCallbacks = [];

		protected $insertCallbacks = [];

		protected $insertOrUpdateCallbacks = [];

		protected $lastBatchId;

		protected $updateCallbackWhenFields = [];

		protected $bypassModel = false;

		protected $rawComparators = [];

		/**
		 * @var TransactionManager
		 */
		protected $transactionManager;

		/**
		 * Creates a new instance
		 * @param Model $model The model
		 */
		public function __construct(Model $model) {

			$pkField = $model->getKeyName();
			if (!$pkField)
				throw new InvalidArgumentException('Batch import requires a primary key. Model ' . get_class($model) . ' does not have a primary key.');

			$this->model = $model;

			// default match by to primary key field, if model has one
			$this->matchBy = [$pkField];
		}


		/**
		 * Sets the fields which must match to treat records as identical. Attention: following SQL null comparison behaviour, a null value in any of these fields will never match any other field value (not even another null value)
		 * @param string[]|callable[]|string $fields The fields. If a callable is passed, it will be used to process the field value before comparison. In that case the array key is interpreted as field name
		 * @return $this
		 */
		public function matchBy($fields): BatchImport {

			$fields = Arr::wrap($fields);

			if (count($fields) < 1)
				throw new InvalidArgumentException('At least one match by field is required');

			$this->assertFieldList($fields);

			$this->matchBy = $fields;

			return $this;
		}

		/**
		 * Sets the fields to update if a given record already exists in database.
		 * @param string[]|callable[]|string $fields The fields. If a callable is passed it will receive the new value and the existing record and must return the value to be written
		 * @return $this
		 */
		public function updateIfExists($fields): BatchImport {

			$fields = Arr::wrap($fields);

			$this->assertFieldList($fields, false);

			$this->updateFields = $fields;

			return $this;
		}

		/**
		 * Sets a list of fields to take into account when determining for which records to invoke the update callback
		 * @param string[] $fields The fields
		 * @return $this
		 */
		public function onUpdatedWhen(array $fields): BatchImport {
			$this->updateCallbackWhenFields = $fields;

			return $this;
		}

		/**
		 * Adds a callback which receives all updated records
		 * @param callable $callback The callback
		 * @return $this
		 */
		public function onUpdated(callable $callback): BatchImport {
			$this->updateCallbacks[] = $callback;

			return $this;
		}

		/**
		 * Adds a callback which receives all inserted records
		 * @param callable $callback The callback
		 * @return $this
		 */
		public function onInserted(callable $callback): BatchImport {
			$this->insertCallbacks[] = $callback;

			return $this;
		}

		/**
		 * Adds a callback which receives all inserted or updated records
		 * @param callable $callback The callback
		 * @return $this
		 */
		public function onInsertedOrUpdated(callable $callback): BatchImport {
			$this->insertOrUpdateCallbacks[] = $callback;

			return $this;
		}


		/**
		 * Sets a custom batch id for the batch import
		 * @param string|int $batchId The batch id
		 * @param string|null $batchIdField The batch id field. If null, the model's batch id field name or 'last_batch_id' is used
		 * @return $this
		 */
		public function withBatchId($batchId, string $batchIdField = null): BatchImport {
			$this->batchId      = $batchId;
			$this->batchIdField = $batchIdField;

			return $this;
		}

		/**
		 * Sets the internal buffer sizes
		 * @param int $bufferSize The buffer size
		 * @param int|null $callbackBufferSize The size for the callback buffer. If null, it is set to the same as the buffer size
		 * @return $this
		 */
		public function buffer(int $bufferSize, int $callbackBufferSize = null): BatchImport {

			if ($bufferSize < 1)
				throw new InvalidArgumentException('Buffer size must be greater than 0.');
			if ($callbackBufferSize !== null && $callbackBufferSize < 1)
				throw new InvalidArgumentException('Callback buffer size must be greater than 0.');

			if ($callbackBufferSize === null)
				$callbackBufferSize = $bufferSize;

			$this->bufferSize         = $bufferSize;
			$this->callbackBufferSize = $callbackBufferSize;

			return $this;
		}

		/**
		 * Performs a batch import for the given models
		 * @param Model[]|iterable|callable $records The records to import
		 * @param string|null $lastBatchId Returns the last used batch id
		 * @return $this
		 */
		public function import($records, &$lastBatchId = null): BatchImport {

			// prepare import
			$import = $this->prepare();

			// if callable given, execute it and use return value
			if (is_callable($records))
				$records = call_user_func($records);

			// add records
			$import->addMultiple($records);

			// flush anything
			$import->flush($lastBatchId);

			return $this;
		}

		/**
		 * Sets if to bypass the model when building import data. This significantly improves performance but has the drawback that attribute values are not processed using casts, mutators and so on.
		 * @param bool $value True if to activate model bypass.
		 * @param callable[] $rawComparators Allows to define custom comparators for fields to detect changes. The comparator function receives the new and the existing value and must return a falsy value if both values are equivalent. Fields without comparator are compared using equality comparison.
		 * @return $this This instance
		 */
		public function bypassModel(bool $value = true, array $rawComparators = []): BatchImport {
			if ($value)
				$this->rawComparators = $rawComparators;
			else if ($rawComparators)
				throw new InvalidArgumentException('Raw comparators are only applicable when bypassModel is activated');

			$this->bypassModel = $value;

			return $this;
		}

		/**
		 * Creates a new prepared import to be executed later
		 * @return PreparedBatchImport
		 */
		public function prepare(): PreparedBatchImport {

			// init buffers
			[$importBuffer, $insertCallbackBuffer, $updateCallbackBuffer, $insertOrUpdateCallbackBuffer] = $this->makeBuffers();

			// export last batch id
			$lastBatchId = $this->lastBatchId;

			$cb = function() use ($importBuffer, $insertCallbackBuffer, $updateCallbackBuffer, $insertOrUpdateCallbackBuffer, $lastBatchId) {

				// flush buffer
				$importBuffer->flush();

				// flush callback buffers
				$insertCallbackBuffer->flush();
				$updateCallbackBuffer->flush();
				$insertOrUpdateCallbackBuffer->flush();

				return $lastBatchId;
			};

			return new PreparedBatchImport($importBuffer, $cb);
		}


		/**
		 * Gets the last used batch id
		 * @return string|null The last used batch id
		 */
		public function getLastBatchId(): ?string {
			return $this->lastBatchId;
		}

		/**
		 * Creates the buffers for importing data
		 * @return FlushingBuffer[] The import buffer, the insert callback buffer, the update callback buffer and the combined buffer
		 */
		protected function makeBuffers() {

			$keyName = $this->model()->getKeyName();

			$bypassModel = $this->bypassModel;

			$updateFields = $this->updateFields;
			$updateFieldNames = $this->fieldNames($updateFields);

			$updatedAtField = null;
			$createdAtField = null;
			if ($this->model()->usesTimestamps()) {
				$updatedAtField = $this->model()->getUpdatedAtColumn();
				$createdAtField = $this->model()->getCreatedAtColumn();
			}

			$updateCallbackWhenFieldsMap = array_fill_keys($this->updateCallbackWhenFields, true);

			$updateCallbackBuffer = new FlushingBuffer($this->callbackBufferSize, function($records) {
				foreach($this->updateCallbacks as $callback) {
					call_user_func($callback, $records);
				}
			});

			$insertCallbackBuffer = new FlushingBuffer($this->callbackBufferSize, function($records) {
				foreach($this->insertCallbacks as $callback) {
					call_user_func($callback, $records);
				}
			});

			$insertOrUpdateCallbackBuffer = new FlushingBuffer($this->callbackBufferSize, function($records) {
				foreach($this->insertOrUpdateCallbacks as $callback) {
					call_user_func($callback, $records);
				}
			});

			$this->lastBatchId = $batchId = $this->batchId();
			$batchIdField      = $this->batchIdField();

			$importBuffer = new FlushingBuffer($this->bufferSize, function($models) use ($updateFieldNames, $updateCallbackBuffer, $insertCallbackBuffer, $insertOrUpdateCallbackBuffer, $batchId, $batchIdField, $updateCallbackWhenFieldsMap, $updateFields, $keyName, $updatedAtField, $createdAtField, $bypassModel) {
				/** @var Model[]|array[] $models */

				$this->withTransaction(function() use ($models, $updateFieldNames, $updateCallbackBuffer, $insertCallbackBuffer, $insertOrUpdateCallbackBuffer, $batchId, $batchIdField, $updateCallbackWhenFieldsMap, $updateFields, $keyName, $updatedAtField, $createdAtField, $bypassModel) {

					$dbData = $this->loadExistingRecords($models);

					$toInsert           = [];
					$toUpdate           = [];
					$toUpdateBatchIdFor = [];
					$updateCallbacksFor = [];

					$now = Carbon::now();

					foreach ($models as $record) {

						if ($bypassModel && $record instanceof Model)
							throw new RuntimeException('Expected data array instead of model when bypassModel is activated');


						// get existing record (by comparison key)
						$comparisonKey  = $this->comparisonKey($record);
						$existingRecord = $comparisonKey !== null ?
							($dbData[$comparisonKey] ?? null) :
							null;


						if ($existingRecord) {
							// apply updates

							$isDirty                     = false;
							$shouldInvokeUpdateCallbacks = false;

							foreach ($updateFields as $key => $field) {

								if (is_int($key)) {
									$key   = $field;
									$value = $record[$field] ?? null;
								}
								else {
									$value = is_callable($field) ?
										call_user_func($field, ($record[$key] ?? null), $existingRecord, $record, $field) :
										$field;
								}


								// Update dirty state. We can save the effort, if already marked as dirty.
								if (!$isDirty || (!$shouldInvokeUpdateCallbacks && ($updateCallbackWhenFieldsMap[$key] ?? false))) {

									// check if current field is dirty
									$isFieldDirty = $this->isFieldModified($key, $value, $existingRecord);

									// set new dirty state if field is dirty
									if ($isFieldDirty)
										$isDirty = true;

									// Check if to invoke update callbacks. If no updateWhenCallbackFields are defined, this is the same as the dirty state.
									// Otherwise the update callbacks should only be invoked when a listed field is dirty
								    $shouldInvokeUpdateCallbacks = !$updateCallbackWhenFieldsMap ?
									    $isDirty :
									    $isFieldDirty && ($updateCallbackWhenFieldsMap[$key] ?? false);
								}

								$existingRecord[$key] = $value;

							}

							if ($isDirty) {

								// set batch id if given
								if ($batchId !== null)
									$existingRecord[$batchIdField] = $batchId;

								// set updated_at date
								if ($updatedAtField)
									$existingRecord[$updatedAtField] = $now;

								$toUpdate[] = $existingRecord;

								// add to list to invoke update callbacks for
								if ($shouldInvokeUpdateCallbacks)
									$updateCallbacksFor[] = $existingRecord;
							}
							else {
								// the record is unchanged, but never the less we have to update the batch id if one given

								if ($batchId !== null)
									$toUpdateBatchIdFor[] = $existingRecord[$keyName];
							}
						}
						else {

							// set batch id if given
							if ($batchId !== null)
								$record[$batchIdField] = $batchId;

							// set timestamp fields
							if ($updatedAtField)
								$record[$updatedAtField] = $now;
							if ($createdAtField)
								$record[$createdAtField] = $now;

							// insert record
							$toInsert[] = $record;
						}

					}

					// update existing records
					if ($toUpdate) {

						if (!$this->bypassModel) {
							// use model update

							$this->callModelStatic('updateWithJoinedModels', $toUpdate, [], array_merge(
								$updateFieldNames,
								($batchId ? [$batchIdField] : [])
							));
						}
						else {
							// use raw update

							$this->modelQuery()
								->toBase()
								->updateWithJoinedData($toUpdate, [$keyName], array_merge(
									$updateFieldNames,
									($batchId ? [$batchIdField] : []),
									($updatedAtField ? [$updatedAtField] : [])
								));
						}
					}


					// update batch id for unmodified records
					if ($toUpdateBatchIdFor) {
						foreach(array_chunk($toUpdateBatchIdFor, $this->bufferSize) as $currKeys) {
							$this->modelQuery()
								->whereIn($keyName, $currKeys)
								->toBase()
								->update([$batchIdField => $batchId]);
						}
					}

					// insert new records
					if ($toInsert) {

						if (!$this->bypassModel) {
							// use model insert

							$this->callModelStatic('insertModels', $toInsert);
						}
						else {
							// use raw insert

							$this->modelQuery()
								->toBase()
								->insert($toInsert);
						}
					}


					// invoke callbacks
					$insertCallbackBuffer->addMultiple($toInsert);
					$updateCallbackBuffer->addMultiple($updateCallbacksFor);
					$insertOrUpdateCallbackBuffer->addMultiple($toInsert);
					$insertOrUpdateCallbackBuffer->addMultiple($updateCallbacksFor);
				});


			});


			return [
				$importBuffer,
				$insertCallbackBuffer,
				$insertOrUpdateCallbackBuffer,
				$updateCallbackBuffer,
			];
		}

		/**
		 * Gets the update field names
		 * @param array $fieldMappings The field mappings
		 * @return string[] The field names
		 */
		protected function fieldNames(array $fieldMappings) {
			$ret = [];

			foreach($fieldMappings as $key => $field) {
				$ret[] = (is_int($key) ? $field : $key);
			}

			return $ret;
		}

		/**
		 * Checks if the given field is modified
		 * @param string $key The key
		 * @param mixed $value The value
		 * @param array|Model $existingRecord The existing record
		 * @return bool True if modified. Else false.
		 */
		protected function isFieldModified($key, $value, $existingRecord): bool {

			// if a model is passed, we use originalIsEquivalent() method
			if (!$this->bypassModel)
				return !$existingRecord->originalIsEquivalent($key, $value);

			$existingValue = $existingRecord[$key] ?? null;

			// use comparator if one exists
			if ($comparator = ($this->rawComparators[$key] ?? null))
				return call_user_func($comparator, $value, $existingValue, $existingRecord);

			// default comparison
			return $value === null && $existingValue === null ?
				false :
				$value != $existingValue;
		}

		/**
		 * Loads the existing records for the given data chunk
		 * @param Model[]|array[] $models The data
		 * @return Model[]|array[] The existing models or model data. Comparison key as array key. Models without valid comparison key are not returned.
		 */
		protected function loadExistingRecords(array $models): array {

			$ret = [];
			$bypassModel = $this->bypassModel;

			/** @var Builder $query */
			$query = $this->existingChunkWhere($this->callModelStatic('query'), $models)
				->lockForUpdate();

			// use query builder if to ignore casts
			if ($bypassModel)
				$query = $query->toBase();

			foreach($query->get() as $curr) {

				// convert stdClass to array
				if ($bypassModel)
					$curr = (array)$curr;

				if (($comparisonKey = $this->comparisonKey($curr)) !== null)
					$ret[$comparisonKey] = $curr;
			}

			return $ret;
		}

		/**
		 * Builds a comparison key for the given record
		 * @param Model $record The record
		 * @return string|null The comparision key. Null if never matches other records (due to null values)
		 */
		protected function comparisonKey($record) {

			$values = [];

			foreach($this->matchBy as $key => $field) {

				$currValue = is_int($key) ?
					($record[$field] ?? null) :
					call_user_func($field, $record[$key] ?? null, $record);


				// if we have a null value, the record is always unique (due to SQL null comparision logic) so we return null
				if ($currValue === null)
					return null;

				// escape field separator and add to values
				$values[] = str_replace(['~', '|'], ['~~', '~'], (string)$currValue);
			}

			return implode('|', $values);
		}

		/**
		 * Applies the where conditions to a query to fetch existing records for the given data chunk
		 * @param Builder $query The query
		 * @param Model[] $models The data chunk
		 * @return Builder The query
		 */
		protected function existingChunkWhere($query, array $models) {

			foreach($this->matchBy as $key => $field) {

				// Skip associative match definitions. They are used for filters which cannot be applied in SQL, so we cannot use them as conditions)
				if (!is_int($key))
					continue;

				// collect values for the current field
				$fieldValues = [];
				$hasNull     = false;
				foreach ($models as $record) {
					$currFieldValue = $record[$field] ?? null;

					// separate null values and others
					if ($currFieldValue !== null)
						$fieldValues[] = $currFieldValue;
					else
						$hasNull = true;
				}
				$fieldValues = array_unique($fieldValues, SORT_STRING);

				// build the where condition for the current field
				$this->whereInValues($query, $field, $fieldValues, $hasNull);

			}

			return $query;
		}

		/**
		 * Apply where conditions for the given field values to a query
		 * @param Builder $query The query
		 * @param string $field The field name
		 * @param array $values The field values. This array must not contain any null values
		 * @param bool $matchNull True if to match null values
		 * @return Builder The query
		 */
		protected function whereInValues($query, string $field, array $values, bool $matchNull = false) {

			// if to match null values, we have to  use 'or ... is null' due to null treatment in SQL comparision
			if ($matchNull) {
				return $query->whereNested(function($query) use ($field, $values) {
					$this
						->whereInValues($query, $field, $values)
						->orWhereNull($field);
				});
			}

			switch(count($values)) {
				case 0:
					return $query;

				case 1:
					return $query->where($field, Arr::first($values));

				default:
					return $query->whereIn($field, $values);

			}
		}

		/**
		 * Execute the given callback within a database transaction
		 * @param callable $callback The callback
		 * @return mixed The callback return
		 * @throws Throwable
		 */
		protected function withTransaction(callable $callback) {
			return $this->transactionManager()->run($this->model, $callback);
		}

		/**
		 * Calls the specified static model method with given args
		 * @param string $method The method name
		 * @param mixed ...$args The arguments
		 * @return mixed The method return value
		 */
		protected function callModelStatic(string $method, ...$args) {
			return forward_static_call([get_class($this->model), $method], ...$args);
		}

		/**
		 * Creates a new model query
		 * @return Builder The query
		 */
		protected function modelQuery() {
			return $this->callModelStatic('query');
		}

		/**
		 * Checks for a correct field list
		 * @param string[]|callable[] $fields The fields
		 * @param bool $onlyCallablesForAssociative False if to allow other associative values then callables
		 */
		protected function assertFieldList(array $fields, bool $onlyCallablesForAssociative = true) {

			foreach ($fields as $key => $value) {
				if (is_int($key)) {
					if (!is_string($value))
						throw new InvalidArgumentException('Expected string for match field with integer key, got ' . gettype($value));
				}
				else {
					if ($onlyCallablesForAssociative && !is_callable($value))
						throw new InvalidArgumentException('Expected callable for match field with string key, got ' . gettype($value));
					if (trim($key) === '')
						throw new InvalidArgumentException('Empty match field name given.');
				}
			}
		}

		/**
		 * @inheritDoc
		 */
		protected function model() {
			return $this->model;
		}

		/**
		 * Gets a transaction manager instance
		 * @return TransactionManager
		 */
		protected function transactionManager() {
			if (!$this->transactionManager)
				$this->transactionManager = app(TransactionManager::class);

			return $this->transactionManager;
		}

	}