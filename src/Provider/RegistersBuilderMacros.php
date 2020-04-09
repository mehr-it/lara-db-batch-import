<?php


	namespace MehrIt\LaraDbBatchImport\Provider;


	use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
	use MehrIt\LaraDbBatchImport\Eloquent\WhereMissingAfterBatch;

	trait RegistersBuilderMacros
	{
		/**
		 * Gets the eloquent builder class
		 * @return string The eloquent builder class
		 */
		protected function getEloquentBuilderClass(): string {
			return EloquentBuilder::class;
		}

		/**
		 * Registers the macros for the eloquent builder
		 */
		protected function registerEloquentBuilderMacros() {

			$cls = $this->getEloquentBuilderClass();

			$this->registerMacro(
				$cls,
				'whereMissingAfterBatch',
				/**
				 * Adds a where condition to match only records which have been missing since the given batch
				 * @param string|null $batchId The batch id
				 * @param string|null $batchIdField The batch if field if it should be set manually
				 * @return \Illuminate\Database\Eloquent\Builder
				 */
				function (string $batchId, string $batchIdField = null) {

					/** @noinspection PhpParamsInspection */
					return (new WhereMissingAfterBatch($this, $batchId, $batchIdField))->apply();
				}
			);

		}


		/**
		 * Registers a macro for the given class
		 * @param string $class The class
		 * @param string $name The macro name
		 * @param callable $callback The callback
		 */
		protected function registerMacro($class, string $name, $callback) {

			forward_static_call([$class, 'macro'], $name, $callback);

		}
	}