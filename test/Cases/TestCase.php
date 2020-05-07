<?php


	namespace MehrItLaraDbBatchImportTest\Cases;


	use Carbon\Carbon;
	use Illuminate\Support\Arr;
	use MehrIt\LaraDbBatchImport\Provider\LaraDbBatchImportProvider;
	use MehrIt\LaraDbExt\Provider\LaraDbExtServiceProvider;
	use MehrIt\LaraTransactions\Provider\TransactionsProvider;

	class TestCase extends \Orchestra\Testbench\TestCase
	{
		use CreatesTestDatabase;

		protected function cleanTables() {

		}

		protected function expectedCallback($times = 1, $args = [], string $name = 'callback', $return = null) {

			$mockBuilder = $this->getMockBuilder('stdClass');

			if (is_callable([$mockBuilder, 'addMethods']))
				$mockBuilder->addMethods([$name]);
			else
				$mockBuilder->setMethods([$name]);

			$mock       = $mockBuilder->getMock();
			$mockMethod = $mock->expects(is_int($times) ? $this->exactly($times) : $times)
				->method($name)
				->willReturn($return);

			if (count($args) > 0)
				$mockMethod->with(...$args);

			return [$mock, $name];
		}

		protected function itemsSubsetMatchesCallback($expected, $exclude = []) {

			$exclude = array_fill_keys($exclude, true);

			return $this->callback(function($value) use ($expected, $exclude) {

				foreach($value as $index => $currItem) {
					$relevant = array_diff_key(array_intersect_key($currItem, $expected[$index]), $exclude);

					$this->assertEquals(array_diff_key($expected[$index], $exclude), $relevant);
				}

				return true;
			});

		}

		protected function matchesExpectedSql($expectedSql, $actual) {
			$valueNorm = $this->normalizeSql($actual);


			$expectedSql = Arr::wrap($expectedSql);

			foreach ($expectedSql as $curr) {
				if ($this->normalizeSql($curr) === $valueNorm)
					return true;
			}

			$this->assertEquals($actual, $expectedSql[0]);

			return false;
		}

		protected function normalizeSql($sql) {
			$sql = preg_replace('/([^\w])\s+/', '$1', $sql);
			$sql = preg_replace('/\s+([^\w])/', '$1', $sql);

			$sql = preg_replace('/\s+/', ' ', $sql);


			return $sql;
		}

		protected function setUp(): void {
			parent::setUp();

			$this->cleanTables();

			// rest test now
			Carbon::setTestNow();

			$this->withFactories(__DIR__ . '/../database/factories');
		}


		/**
		 * @inheritDoc
		 */
		protected function setUpTraits() {
			$this->setupTestingMigrations(__DIR__ . '/../database/migrations');

			return parent::setUpTraits();
		}

		/**
		 * Load package service provider
		 * @param \Illuminate\Foundation\Application $app
		 * @return array
		 */
		protected function getPackageProviders($app) {
			return [
				LaraDbBatchImportProvider::class,
				LaraDbExtServiceProvider::class,
				TransactionsProvider::class,
			];
		}


	}