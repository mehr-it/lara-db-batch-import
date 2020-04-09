<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\Model;
	use MehrIt\LaraDbBatchImport\Model\ProvidesBatchImport;

	class TestModelProvidingBatchImport extends Model
	{
		use ProvidesBatchImport;

	}