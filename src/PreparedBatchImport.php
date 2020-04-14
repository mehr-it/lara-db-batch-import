<?php


	namespace MehrIt\LaraDbBatchImport;


	use Illuminate\Database\Eloquent\Model;
	use InvalidArgumentException;
	use Traversable;

	class PreparedBatchImport
	{

		/**
		 * @var BatchImport
		 */
		protected $import;


		protected $data = [];

		/**
		 * Creates a new instance
		 * @param BatchImport $import The import instance
		 */
		public function __construct(BatchImport $import) {
			$this->import = $import;
		}

		/**
		 * Gets the prepared batch import instance
		 * @return BatchImport The prepared batch import instance
		 */
		public function getImport(): BatchImport {
			return $this->import;
		}



		/**
		 * Adds the given record to the batch import
		 * @param Model $record The record
		 * @return $this
		 */
		public function add(Model $record): PreparedBatchImport {
			$this->data[] = $record;

			return $this;
		}

		/**
		 * Adds multiple records
		 * @param Model[]|iterable $records The records
		 * @return $this
		 */
		public function addMultiple($records): PreparedBatchImport {

			if (!is_array($records) && !($records instanceof Traversable))
				throw new InvalidArgumentException('Expected array or traversable, got ' . is_object($records) ? get_class($records) : strtolower(gettype($records)));

			$this->data[] = $records;

			return $this;
		}


		/**
		 * Flushes all data in the prepared import
		 * @param string|null $lastBatchId Returns the last used batch id
		 * @return $this
		 */
		public function flush(&$lastBatchId = null): PreparedBatchImport {

			// build a generator for all given items
			$gen = function () {
				foreach ($this->data as $curr) {
					if ($curr instanceof Model) {
						// current is a single model => yield it
						yield $curr;
					}
					else {
						// current is an iterator => yield items

						foreach ($curr as $currItem) {
							yield $currItem;
						}
					}
				}
			};

			// perform import
			$this->import->import($gen(), $lastBatchId);

			return $this;
		}

	}