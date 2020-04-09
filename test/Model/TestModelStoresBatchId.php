<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\Model;
	use MehrIt\LaraDbBatchImport\Contracts\StoresBatchId;

	class TestModelStoresBatchId extends Model implements StoresBatchId
	{
		protected $table = 'test_table';

		public function getBatchIdField(): string {
			return 'my_batch_id_field';
		}


	}