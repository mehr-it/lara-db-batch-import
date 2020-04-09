<?php


	namespace MehrIt\LaraDbBatchImport\Concerns;


	use Illuminate\Database\Eloquent\Model;
	use MehrIt\LaraDbBatchImport\Contracts\GeneratesBatchIds;
	use MehrIt\LaraDbBatchImport\Contracts\StoresBatchId;

	trait BatchImportInfo
	{
		protected $batchIdField = null;

		protected $batchId = null;

		/**
		 * Gets the model
		 * @return Model|null The model
		 */
		protected abstract function model();

		/**
		 * Gets the next batch id
		 * @return string|null The next batch id
		 */
		protected function batchId(): ?string {

			// batch id disabled?
			if ($this->batchId === false)
				return null;

			// batch id manually set?
			if ($this->batchId !== null)
				return (string)$this->batchId;

			// use model to generate, if possible
			$model = $this->model();
			if ($model && $model instanceof GeneratesBatchIds)
				return $model->nextBatchId();

			return null;
		}

		/**
		 * Gets the batch id field
		 * @return string|null The batch id field
		 */
		protected function batchIdField(): ?string {

			// manually set?
			if ($this->batchIdField !== null)
				return $this->batchIdField;

			// get from model is possible
			$model = $this->model();
			if ($model && $model instanceof StoresBatchId)
				return $model->getBatchIdField();

			return 'last_batch_id';
		}
	}