<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\Model;
	use MehrIt\LaraDbBatchImport\Contracts\GeneratesBatchIds;
	use MehrIt\LaraDbBatchImport\Contracts\StoresBatchId;

	class TestModelWithBatch extends Model implements GeneratesBatchIds, StoresBatchId
	{

		protected $table = 'test_table';

		protected $guarded = [];

		public function nextBatchId(): string {
			return '25';
		}

		public function getBatchIdField(): string {
			return 'c';
		}


	}