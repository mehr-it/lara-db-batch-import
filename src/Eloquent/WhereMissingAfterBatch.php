<?php


	namespace MehrIt\LaraDbBatchImport\Eloquent;


	use Illuminate\Database\Eloquent\Builder;
	use InvalidArgumentException;
	use MehrIt\LaraDbBatchImport\Concerns\BatchImportInfo;
	use MehrIt\LaraDbBatchImport\Contracts\StoresBatchId;
	use RuntimeException;

	class WhereMissingAfterBatch
	{
		use BatchImportInfo;

		/**
		 * @var Builder
		 */
		protected $builder;

		/**
		 * Creates a new instance
		 * @param Builder $builder The query
		 * @param string|null $batchId The batch id
		 * @param string|null $batchIdField The batch if field if it should be set manually
		 */
		public function __construct(Builder $builder, string $batchId, string $batchIdField = null) {
			$this->builder = $builder;

			$this->batchId      = $batchId;
			$this->batchIdField = $batchIdField;
		}

		/**
		 * Applies the necessary conditions to the query
		 * @return Builder The query
		 */
		public function apply() {

			$model = $this->model();
			if (!$model)
				throw new RuntimeException('Model must be passed to builder before using this function');

			$field = $this->batchIdField();

			return $this->builder->whereNested(function($query) use ($field, $model) {
				/** @var Builder $query */
				$query
					->where($model->qualifyColumn($field), '<', $this->batchId)
					->orWhereNull($model->qualifyColumn($field));
			});
		}

		/**
		 * @inheritDoc
		 */
		protected function model() {
			return $this->builder->getModel();
		}


	}