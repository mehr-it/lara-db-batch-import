<?php


	namespace MehrItLaraDbBatchImportTest\Model;


	use Illuminate\Database\Eloquent\Model;

	class TestModelWithoutPrimaryKey extends Model
	{
		protected $primaryKey = null;
	}