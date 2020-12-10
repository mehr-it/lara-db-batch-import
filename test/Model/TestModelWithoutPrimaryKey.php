<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\Model;
	use MehrIt\LaraDbExt\Model\DbExtensions;

	class TestModelWithoutPrimaryKey extends Model
	{
		use DbExtensions;

		protected $primaryKey = null;
	}