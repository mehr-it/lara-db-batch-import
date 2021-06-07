<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\SoftDeletes;

	class TestModelWithSoftDelete extends TestModel
	{
		use SoftDeletes;
	}