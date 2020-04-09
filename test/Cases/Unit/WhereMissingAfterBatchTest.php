<?php


	namespace MehrItLaraDbBatchImportTest\Cases\Unit;


	use Illuminate\Database\Connection;
	use Illuminate\Database\ConnectionInterface;
	use Illuminate\Database\ConnectionResolverInterface;
	use Illuminate\Database\Eloquent\Builder as EloquentBuilder;
	use Illuminate\Database\Eloquent\Model;
	use Illuminate\Database\Query\Builder as QueryBuilder;
	use Illuminate\Database\Query\Grammars\Grammar;
	use Illuminate\Database\Query\Processors\Processor;
	use MehrIt\LaraDbBatchImport\Eloquent\WhereMissingAfterBatch;
	use MehrItLaraDbBatchImportTest\Cases\TestCase;
	use MehrItLaraDbBatchImportTest\Model\TestModel;
	use MehrItLaraDbBatchImportTest\Model\TestModelStoresBatchId;
	use PHPUnit\Framework\MockObject\MockObject;

	class WhereMissingAfterBatchTest extends TestCase
	{

		/**
		 * Gets a new query builder instance
		 * @param ConnectionInterface|MockObject|null $connectionInterface
		 * @param null|Model $model The model
		 * @return EloquentBuilder
		 */
		protected function getBuilder(&$connectionInterface = null, $model = null) {
			$grammar = new Grammar();

			/** @var Processor|MockObject $processor */
			$processor = $this->getMockBuilder(Processor::class)->getMock();

			/** @var Connection|MockObject $connectionInterface */
			$connectionInterface = $this->getMockBuilder(Connection::class)->disableOriginalConstructor()->getMock();
			$connectionInterface
				->method('getQueryGrammar')
				->willReturn($grammar);

			/** @var ConnectionResolverInterface|MockObject $resolver */
			$resolver = $this->getMockBuilder(ConnectionResolverInterface::class)->getMock();
			$resolver
				->method('connection')
				->willReturn($connectionInterface);


			$builder = new EloquentBuilder(new QueryBuilder($connectionInterface, $grammar, $processor));

			$model = $model ?: new TestModel();

			// set connection resolver for model
			forward_static_call([$model, 'setConnectionResolver'], $resolver);

			$builder->setModel($model);

			return $builder;
		}

		public function testWhereMissingAfterBatch() {

			/** @var ConnectionInterface|MockObject $connectionInterface */
			$builder = $this->getBuilder($connectionInterface);


			$expectedSql      = 'select * from "test_table" where ("test_table"."last_batch_id" < ? or "test_table"."last_batch_id" is null)';
			$expectedBindings = [
				19
			];

			$query = (new WhereMissingAfterBatch($builder, 19))->apply();

			$this->assertTrue($this->matchesExpectedSql(
				$expectedSql,
				$query->toSql()
			));
			$this->assertEquals($expectedBindings, $query->getBindings());

		}

		public function testWhereMissingAfterBatch_customBatchIdField() {

			/** @var ConnectionInterface|MockObject $connectionInterface */
			$builder = $this->getBuilder($connectionInterface);


			$expectedSql      = 'select * from "test_table" where ("test_table"."my_field" < ? or "test_table"."my_field" is null)';
			$expectedBindings = [
				19
			];

			$query = (new WhereMissingAfterBatch($builder, 19, 'my_field'))->apply();

			$this->assertTrue($this->matchesExpectedSql(
				$expectedSql,
				$query->toSql()
			));
			$this->assertEquals($expectedBindings, $query->getBindings());

		}

		public function testWhereMissingAfterBatch_fieldFromModel() {

			/** @var ConnectionInterface|MockObject $connectionInterface */
			$builder = $this->getBuilder($connectionInterface, new TestModelStoresBatchId());


			$expectedSql      = 'select * from "test_table" where ("test_table"."my_batch_id_field" < ? or "test_table"."my_batch_id_field" is null)';
			$expectedBindings = [
				19
			];

			$query = (new WhereMissingAfterBatch($builder, 19))->apply();

			$this->assertTrue($this->matchesExpectedSql(
				$expectedSql,
				$query->toSql()
			));
			$this->assertEquals($expectedBindings, $query->getBindings());

		}

		public function testWhereMissingAfterBatch_fieldFromModel_overwrittenWithCustom() {

			/** @var ConnectionInterface|MockObject $connectionInterface */
			$builder = $this->getBuilder($connectionInterface, new TestModelStoresBatchId());


			$expectedSql      = 'select * from "test_table" where ("test_table"."my_field" < ? or "test_table"."my_field" is null)';
			$expectedBindings = [
				19
			];

			$query = (new WhereMissingAfterBatch($builder, 19, 'my_field'))->apply();

			$this->assertTrue($this->matchesExpectedSql(
				$expectedSql,
				$query->toSql()
			));
			$this->assertEquals($expectedBindings, $query->getBindings());

		}

	}