<?php


	namespace MehrIt\LaraDbBatchImport;


	use Illuminate\Database\Eloquent\Builder;
	use Illuminate\Database\Eloquent\Model;
	use Illuminate\Support\Arr;
	use InvalidArgumentException;
	use MehrIt\Buffer\FlushingBuffer;
	use MehrIt\LaraDbBatchImport\Concerns\BatchImportInfo;
	use MehrIt\LaraTransactions\TransactionManager;
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

			$updateFieldNames = $this->fieldNames($this->updateFields);

			$updateCallbackWhenFields = $this->updateCallbackWhenFields;

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

			$importBuffer = new FlushingBuffer($this->bufferSize, function($models) use ($updateFieldNames, $updateCallbackBuffer, $insertCallbackBuffer, $insertOrUpdateCallbackBuffer, $batchId, $batchIdField, $updateCallbackWhenFields) {
				/** @var Model[] $models */

				$this->withTransaction(function() use ($models, $updateFieldNames, $updateCallbackBuffer, $insertCallbackBuffer, $insertOrUpdateCallbackBuffer, $batchId, $batchIdField, $updateCallbackWhenFields) {

					$dbData = $this->loadExistingRecords($models);

					$toInsert           = [];
					$toUpdate           = [];
					$toUpdateBatchIdFor = [];
					$updateCallbacksFor = [];

					foreach ($models as $record) {

						// get existing record (by comparison key)
						$comparisonKey  = $this->comparisonKey($record);
						$existingRecord = $comparisonKey !== null ?
							($dbData[$comparisonKey] ?? null) :
							null;


						if ($existingRecord) {
							// apply updates

							foreach ($this->updateFields as $key => $field) {

								if (is_int($key))
									$existingRecord[$field] = ($record[$field] ?? null);
								else
									$existingRecord[$key] = (is_callable($field) ? call_user_func($field, ($record[$key] ?? null), $existingRecord, $record, $field) : $field);
							}

							if ($existingRecord->isDirty($updateFieldNames)) {

								// set batch id if given
								if ($batchId !== null)
									$existingRecord[$batchIdField] = $batchId;

								$toUpdate[] = $existingRecord;

								// add to list to invoke update callbacks for
								if (!$updateCallbackWhenFields || $existingRecord->isDirty($updateCallbackWhenFields))
									$updateCallbacksFor[] = $existingRecord;
							}
							else {
								// the record is unchanged, but never the less we have to update the batch id if one given

								if ($batchId !== null)
									$toUpdateBatchIdFor[] = $existingRecord->getKey();
							}
						}
						else {

							// set batch id if given
							if ($batchId !== null)
								$record[$batchIdField] = $batchId;

							// insert record
							$toInsert[] = $record;
						}

					}

					// update existing record
					if ($toUpdate)
						$this->callModelStatic('updateWithJoinedModels', $toUpdate, [], array_merge($updateFieldNames, ($batchId ? [$batchIdField] : [])));


					// update batch id for unmodified records
					if ($toUpdateBatchIdFor) {
						foreach(array_chunk($toUpdateBatchIdFor, $this->bufferSize) as $currKeys) {
							/** @var Builder $query */
							$query = $this->callModelStatic('query');

							$query->whereIn($this->model->getKeyName(), $currKeys)
								->toBase()
								->update([$batchIdField => $batchId]);
						}
					}

					// insert new records
					if ($toInsert)
						$this->callModelStatic('insertModels', $toInsert);


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
		 * Loads the existing records for the given data chunk
		 * @param Model[] $models The data
		 * @return Model[] The existing models. Comparison key as array key. Models without valid comparison key are not returned.
		 */
		protected function loadExistingRecords(array $models): array {

			$ret = [];

			$this->existingChunkWhere($this->callModelStatic('query'), $models)
				->lockForUpdate()
				->get()
				->each(function($record) use (&$ret) {
					$comparisonKey = $this->comparisonKey($record);

					if ($comparisonKey !== null)
						$ret[$comparisonKey] = $record;
				});


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