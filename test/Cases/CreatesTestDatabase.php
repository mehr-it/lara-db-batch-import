<?php


	namespace MehrItLaraDbBatchImportTest\Cases;


	use Illuminate\Foundation\Testing\DatabaseMigrations;
	use Illuminate\Foundation\Testing\DatabaseTransactions;
	use \DB;

	trait CreatesTestDatabase
	{
		protected static $testingMigrationsRun = false;

		/**
		 * Setup database migrations required for testing only
		 * @param string $path The path to load testing migrations from
		 * @param string|null $connection The connection name
		 */
		protected function setupTestingMigrations($path, $connection = null) {
			$uses = array_flip(class_uses_recursive(static::class));

			// only load migrations, if test is using database and not run yet
			if (isset($uses[DatabaseMigrations::class])
			    || (isset($uses[DatabaseTransactions::class]) && (!static::$testingMigrationsRun || DB::getDatabaseName() == ':memory:'))
			) {

				// load migrations to create tables required for testing
				$this->loadTestMigrationsFrom($path, $connection);
			}
		}


		/**
		 * Adds the given path to load database migrations from
		 * @param string|string[] $paths The path(s) to load migrations from
		 * @param string|null $connection The connection name to use. This options seams to be required for migrations using non-default connection. If not set, the migration log is not created
		 */
		protected function loadTestMigrationsFrom($paths, $connection = null) {

			if (!is_array($paths))
				$paths = [$paths];

			foreach ($paths as $currPath) {
				$currOptions = [
					'--path'     => $currPath,
					'--realpath' => true,
				];

				if ($connection) // this options seams to be required for migrations using non-default connection. If not set, the migration log is not created.
					$currOptions['--database'] = $connection;

				// migrate keeping default connection
				$defaultConnection = DB::getDefaultConnection();
				$this->artisan('migrate:fresh', $currOptions);
				DB::setDefaultConnection($defaultConnection);

				static::$testingMigrationsRun = true;

			}
		}
	}