<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\Builder;
	use Illuminate\Database\Eloquent\Model;
	use MehrIt\LaraDbExt\Model\DbExtensions;

	/**
	 * Class TestModel
	 * @package MehrItLaraDbBatchImportTest\Model
	 *
	 * @mixin Builder
	 */
	class TestModel extends Model
	{
		use DbExtensions;

		protected $table = 'test_table';

		protected $guarded = [];
	}