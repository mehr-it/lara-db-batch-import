<?php


	namespace MehrIt\LaraDbBatchImport\Provider;


	use Illuminate\Support\ServiceProvider;
	use MehrIt\LaraDbBatchImport\BatchImport;

	class LaraDbBatchImportProvider extends ServiceProvider
	{
		use RegistersBuilderMacros;

		public function boot() {

			$this->registerEloquentBuilderMacros();

		}

		/** @noinspection PhpUnusedParameterInspection */
		public function register() {

			app()->bind(BatchImport::class, function($app, $params) {
				return new BatchImport($params['model']);
			});
		}
	}