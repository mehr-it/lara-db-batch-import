<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\Model;
	use MehrIt\LaraDbBatchImport\Model\ProvidesBatchImport;
	use MehrIt\LaraDbExt\Model\DbExtensions;

	class TestModelProvidingBatchImport extends Model
	{
		use DbExtensions;

		use ProvidesBatchImport;

	}