<?php


	namespace MehrIt\LaraDbBatchImport;


	use Illuminate\Database\Eloquent\Model;
	use InvalidArgumentException;
	use MehrIt\Buffer\FlushingBuffer;
	use Traversable;

	class PreparedBatchImport
	{


		/**
		 * @var FlushingBuffer
		 */
		protected $buffer;

		/**
		 * @var callable The flush callback
		 */
		protected $flushAllCallback;

		/**
		 * Creates a new instance
		 * @param FlushingBuffer $buffer The buffer
		 * @param callable $flushAllCallback The flush callback flushing any remaining data and returning the last batch id
		 */
		public function __construct(FlushingBuffer $buffer, callable $flushAllCallback) {
			$this->buffer           = $buffer;
			$this->flushAllCallback = $flushAllCallback;
		}


		/**
		 * Adds the given record to the batch import
		 * @param Model $record The record
		 * @return $this
		 */
		public function add(Model $record): PreparedBatchImport {
			$this->buffer->add($record);

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

			$this->buffer->addMultiple($records);

			return $this;
		}


		/**
		 * Flushes all data in the prepared import
		 * @param string|null $lastBatchId Returns the last used batch id
		 * @return $this
		 */
		public function flush(&$lastBatchId = null): PreparedBatchImport {

			$lastBatchId = call_user_func($this->flushAllCallback);

			return $this;
		}

	}