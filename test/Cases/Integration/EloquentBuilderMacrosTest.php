<?php


	namespace MehrItLaraDbBatchImportTest\Cases\Integration;


	use DB;
	use Illuminate\Foundation\Testing\DatabaseTransactions;
	use MehrItLaraDbBatchImportTest\Cases\TestCase;
	use MehrItLaraDbBatchImportTest\Model\TestModel;

	class EloquentBuilderMacrosTest extends TestCase
	{
		use DatabaseTransactions;

		protected function cleanTables() {
			DB::table('test_table')->delete();
		}


		public function testWhereMissingAfterBatch() {

			$testModel1 = factory(TestModel::class)->create([
				'last_batch_id' => 1,
			]);
			$testModel2 = factory(TestModel::class)->create([
				'last_batch_id' => 2,
			]);
			$testModel3 = factory(TestModel::class)->create([
				'last_batch_id' => 3,
			]);
			$testModel4 = factory(TestModel::class)->create([
				'last_batch_id' => null,
			]);


			$ids = array_fill_keys(TestModel::query()->whereMissingAfterBatch(2)->pluck('id')->toArray(), true);

			$this->assertArrayHasKey($testModel1->id, $ids);
			$this->assertArrayNotHasKey($testModel2->id, $ids);
			$this->assertArrayNotHasKey($testModel3->id, $ids);
			$this->assertArrayHasKey($testModel4->id, $ids);

			$this->assertCount(2, $ids);
		}

		public function testWhereMissingAfterBatch_withCustomBatchIdField() {

			$testModel1 = factory(TestModel::class)->create([
				'c' => 1,
			]);
			$testModel2 = factory(TestModel::class)->create([
				'c' => 2,
			]);
			$testModel3 = factory(TestModel::class)->create([
				'c' => 3,
			]);
			$testModel4 = factory(TestModel::class)->create([
				'c' => null,
			]);


			$ids = array_fill_keys(TestModel::query()->whereMissingAfterBatch(2 ,'c')->pluck('id')->toArray(), true);

			$this->assertArrayHasKey($testModel1->id, $ids);
			$this->assertArrayNotHasKey($testModel2->id, $ids);
			$this->assertArrayNotHasKey($testModel3->id, $ids);
			$this->assertArrayHasKey($testModel4->id, $ids);

			$this->assertCount(2, $ids);
		}
	}