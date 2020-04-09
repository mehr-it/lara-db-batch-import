<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\Builder;
	use Illuminate\Database\Eloquent\Model;

	/**
	 * Class TestModel
	 * @package MehrItLaraDbBatchImportTest\Model
	 *
	 * @mixin Builder
	 */
	class TestModel extends Model
	{
		protected $table = 'test_table';

		protected $guarded = [];
	}