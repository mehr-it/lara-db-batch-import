<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\Model;
	use MehrIt\LaraDbExt\Model\DbExtensions;

	class TestModelWithSetMutator extends Model
	{
		use DbExtensions;

		protected $table = 'test_table';

		protected $guarded = [];

		public function setAAttribute($value) {
			$this->attributes['a'] = "mutated:{$value}";
		}
	}