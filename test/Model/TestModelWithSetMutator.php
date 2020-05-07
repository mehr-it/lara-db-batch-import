<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\Model;

	class TestModelWithSetMutator extends Model
	{
		protected $table = 'test_table';

		protected $guarded = [];

		public function setAAttribute($value) {
			$this->attributes['a'] = "mutated:{$value}";
		}
	}