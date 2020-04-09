<?php


	namespace MehrIt\LaraDbBatchImport\Model;


	use MehrIt\LaraDbBatchImport\BatchImport;

	trait ProvidesBatchImport
	{

		/**
		 * Creates a new batch import for this model
		 * @return BatchImport The batch import
		 */
		public static function batchImport(): BatchImport {
			return app(BatchImport::class, [
				'model' => new static,
			]);
		}

	}