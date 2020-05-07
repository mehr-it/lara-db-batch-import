<?php


	namespace MehrItLaraDbBatchImportTest\Cases\Unit;


	use Carbon\Carbon;
	use Illuminate\Foundation\Testing\DatabaseTransactions;
	use \DB;
	use InvalidArgumentException;
	use MehrIt\LaraDbBatchImport\BatchImport;
	use MehrIt\LaraDbBatchImport\PreparedBatchImport;
	use MehrItLaraDbBatchImportTest\Cases\TestCase;
	use MehrItLaraDbBatchImportTest\Model\TestModel;
	use MehrItLaraDbBatchImportTest\Model\TestModelWithBatch;
	use MehrItLaraDbBatchImportTest\Model\TestModelWithoutPrimaryKey;
	use MehrItLaraDbBatchImportTest\Model\TestModelWithSetMutator;

	class BatchImportTest extends TestCase
	{
		use DatabaseTransactions;

		protected function cleanTables() {
			DB::table('test_table')->delete();
		}

		protected function setUp(): void {
			parent::setUp();

			Carbon::setTestNow(Carbon::now());
		}


		public function testBatchImport_insertOnly() {


			$model = new TestModel();

			$testModel1 = new TestModel([
				'a' => 'a1',
				'b' => 'b1',
				'c' => 'd1',
				'd' => Carbon::now(),
			]);
			$testModel2 = new TestModel([
				'a' => 'a2',
				'b' => 'b2',
				'c' => 'd2',
				'd' => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModel1,
						$testModel2,
					],
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(0, [], 'updateCallback'));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					[
						$testModel1,
						$testModel2,
					],
				],
				'insertOrUpdateCallback'
			));


			$this->assertSame($import, $import->import([
				$testModel1,
				$testModel2,
			]));



			$this->assertDatabaseHas('test_table', [
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(2, DB::table('test_table')->count());
		}

		public function testBatchImport_insertOnly_bypassModel() {


			$model = new TestModel();

			$data1 = [
				'a' => 'a1',
				'b' => 'b1',
				'c' => 'd1',
				'd' => Carbon::now(),
			];
			$data2 =[
				'a' => 'a2',
				'b' => 'b2',
				'c' => 'd2',
				'd' => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$data1,
						$data2,
					]),
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(0, [], 'updateCallback'));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$data1,
						$data2,
					]),
				],
				'insertOrUpdateCallback'
			));

			$this->assertSame($import, $import->bypassModel());


			$this->assertSame($import, $import->import([
				$data1,
				$data2,
			]));



			$this->assertDatabaseHas('test_table', [
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(2, DB::table('test_table')->count());
		}

		public function testBatchImport_updateOnly() {


			$model = new TestModel();

			$tm1 = factory(TestModel::class)->create();
			$tm2 = factory(TestModel::class)->create();

			$testModel1 = new TestModel([
				'id' => $tm1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModel2 = new TestModel([
				'id' => $tm2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(0, [], 'insertCallback'));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($testModel1, $testModel2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModel1->id, $actualIdMap);
						$this->assertArrayHasKey($testModel2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModel1, $testModel2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModel1->id, $actualIdMap);
						$this->assertArrayHasKey($testModel2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModel1,
				$testModel2,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $tm1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $tm2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertSame(2, DB::table('test_table')->count());
		}

		public function testBatchImport_updateOnly_bypassModel() {

			$model = new TestModel();

			$tm1 = factory(TestModel::class)->create();
			$tm2 = factory(TestModel::class)->create();

			$data1 = [
				'id' => $tm1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$data2 = [
				'id' => $tm2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(0, [], 'insertCallback'));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($data1, $data2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($data1['id'], $actualIdMap);
						$this->assertArrayHasKey($data2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($data1, $data2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($data1['id'], $actualIdMap);
						$this->assertArrayHasKey($data2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$data1,
				$data2,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $tm1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $tm2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertSame(2, DB::table('test_table')->count());
		}

		public function testBatchImport_updateOnly_onUpdateWhen() {


			$model = new TestModel();

			$tm1 = factory(TestModel::class)->create([
				'a' => 'a1',
			]);
			$tm2 = factory(TestModel::class)->create([
				'a' => 'xxx',
			]);

			$testModel1 = new TestModel([
				'id' => $tm1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModel2 = new TestModel([
				'id' => $tm2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(0, [], 'insertCallback'));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($testModel1, $testModel2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModel2->id, $actualIdMap);
						$this->assertCount(1, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModel1, $testModel2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModel2->id, $actualIdMap);
						$this->assertCount(1, $actualIdMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));


			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));
			$this->assertSame($import, $import->onUpdatedWhen(['a']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModel1,
				$testModel2,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $tm1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $tm2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertSame(2, DB::table('test_table')->count());
		}

		public function testBatchImport_updateOnly_onUpdateWhen_bypassModel() {


			$model = new TestModel();

			$tm1 = factory(TestModel::class)->create([
				'a' => 'a1',
			]);
			$tm2 = factory(TestModel::class)->create([
				'a' => 'xxx',
			]);

			$data1 = [
				'id' => $tm1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$data2 = [
				'id' => $tm2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(0, [], 'insertCallback'));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($data1, $data2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($data2['id'], $actualIdMap);
						$this->assertCount(1, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($data1, $data2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($data2['id'], $actualIdMap);
						$this->assertCount(1, $actualIdMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));


			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));
			$this->assertSame($import, $import->onUpdatedWhen(['a']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$data1,
				$data2,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $tm1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $tm2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertSame(2, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUpdated2 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$testModelUpdate1 = new TestModel([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2 = new TestModel([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($testModelUpdate1, $testModelUpdate2, $testModelToInsert) {
						$actualCMap = [];
						foreach ($value as $curr) {
							$actualCMap[$curr->c] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->c, $actualCMap);
						$this->assertArrayHasKey($testModelUpdate2->c, $actualCMap);
						$this->assertArrayHasKey($testModelToInsert->c, $actualCMap);
						$this->assertCount(3, $actualCMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUpdated2 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$dataUpdate1 = [
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2 = [
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($dataUpdate1, $dataUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['id'], $actualIdMap);
						$this->assertArrayHasKey($dataUpdate2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($dataUpdate1, $dataUpdate2, $dataInsert) {
						$actualCMap = [];
						foreach ($value as $curr) {
							$actualCMap[$curr['c']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['c'], $actualCMap);
						$this->assertArrayHasKey($dataUpdate2['c'], $actualCMap);
						$this->assertArrayHasKey($dataInsert['c'], $actualCMap);
						$this->assertCount(3, $actualCMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withUpdateFieldProcessor() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUpdated2 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$testModelUpdate1 = new TestModel([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2 = new TestModel([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists([
				'a',
				'b',
				'c' => function($v) {
					return $v . '-modified';
				},
				'd',
			]));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1-modified',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2-modified',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withUpdateFieldProcessor_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUpdated2 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$dataUpdate1 = [
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2 = [
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($dataUpdate1, $dataUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['id'], $actualIdMap);
						$this->assertArrayHasKey($dataUpdate2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists([
				'a',
				'b',
				'c' => function($v) {
					return $v . '-modified';
				},
				'd',
			]));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1-modified',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2-modified',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withUpdateFieldStaticValue() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUpdated2 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$testModelUpdate1 = new TestModel([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2 = new TestModel([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists([
				'a',
				'b',
				'c' => 'static-value',
				'd',
			]));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'static-value',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'static-value',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withUpdateFieldStaticValue_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUpdated2 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$dataUpdate1 = [
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2 = [
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($dataUpdate1, $dataUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['id'], $actualIdMap);
						$this->assertArrayHasKey($dataUpdate2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists([
				'a',
				'b',
				'c' => 'static-value',
				'd',
			]));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'static-value',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'static-value',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_customMatchByFields() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUpdated2 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$testModelUpdate1 = new TestModel([
				'a'  => $toBeUpdated1->a,
				'b'  => $toBeUpdated1->b,
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2 = new TestModel([
				'a'  => $toBeUpdated2->a,
				'b'  => $toBeUpdated2->b,
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualFieldCMap = [];
						foreach ($value as $curr) {
							$actualFieldCMap[$curr->c] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->c, $actualFieldCMap);
						$this->assertArrayHasKey($testModelUpdate2->c, $actualFieldCMap);
						$this->assertCount(2, $actualFieldCMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));
			$this->assertSame($import, $import->matchBy(['a', 'b']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => $toBeUpdated1->a,
				'b'          => $toBeUpdated1->b,
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => $toBeUpdated2->a,
				'b'          => $toBeUpdated2->b,
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_customMatchByFields_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUpdated2 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$dataUpdate1 = [
				'a'  => $toBeUpdated1->a,
				'b'  => $toBeUpdated1->b,
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2 = [
				'a'  => $toBeUpdated2->a,
				'b'  => $toBeUpdated2->b,
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($dataUpdate1, $dataUpdate2) {
						$actualFieldCMap = [];
						foreach ($value as $curr) {
							$actualFieldCMap[$curr['c']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['c'], $actualFieldCMap);
						$this->assertArrayHasKey($dataUpdate2['c'], $actualFieldCMap);
						$this->assertCount(2, $actualFieldCMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));
			$this->assertSame($import, $import->matchBy(['a', 'b']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => $toBeUpdated1->a,
				'b'          => $toBeUpdated1->b,
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => $toBeUpdated2->a,
				'b'          => $toBeUpdated2->b,
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_customMatchByFields_singlePassedAsString() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUpdated2 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$testModelUpdate1 = new TestModel([
				'a'  => $toBeUpdated1->a,
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2 = new TestModel([
				'a'  => $toBeUpdated2->a,
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualFieldCMap = [];
						foreach ($value as $curr) {
							$actualFieldCMap[$curr->c] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->c, $actualFieldCMap);
						$this->assertArrayHasKey($testModelUpdate2->c, $actualFieldCMap);
						$this->assertCount(2, $actualFieldCMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));
			$this->assertSame($import, $import->matchBy('a'));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => $toBeUpdated1->a,
				'b'          => $testModelUpdate1->b,
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => $toBeUpdated2->a,
				'b'          => $testModelUpdate2->b,
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_customMatchByFields_singlePassedAsString_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUpdated2 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$dataUpdate1 = [
				'a'  => $toBeUpdated1->a,
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2 =[
				'a'  => $toBeUpdated2->a,
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($dataUpdate1, $dataUpdate2) {
						$actualFieldCMap = [];
						foreach ($value as $curr) {
							$actualFieldCMap[$curr['c']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['c'], $actualFieldCMap);
						$this->assertArrayHasKey($dataUpdate2['c'], $actualFieldCMap);
						$this->assertCount(2, $actualFieldCMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));
			$this->assertSame($import, $import->matchBy('a'));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => $toBeUpdated1->a,
				'b'          => $dataUpdate1['b'],
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => $toBeUpdated2->a,
				'b'          => $dataUpdate2['b'],
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_customMatchByFieldsWithProcessor() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create([
				'a' => '-a1',
				'b' => 'b1-',
			]);
			$toBeUpdated2 = factory(TestModel::class)->create([
				'a' => '-a2',
				'b' => 'b2-',
			]);
			$toBeUnmodified = factory(TestModel::class)->create();

			$testModelUpdate1 = new TestModel([
				'a'  => '+a1',
				'b'  => 'b1+',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2 = new TestModel([
				'a' => '+a2',
				'b' => 'b2+',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualFieldCMap = [];
						foreach ($value as $curr) {
							$actualFieldCMap[$curr->c] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->c, $actualFieldCMap);
						$this->assertArrayHasKey($testModelUpdate2->c, $actualFieldCMap);
						$this->assertCount(2, $actualFieldCMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['c', 'd']));
			$this->assertSame($import, $import->matchBy([
				'a' => function ($v) {
					return substr($v, 1);
				},
				'b' => function ($v) {
					return substr($v, 0, -1);
				}
			]));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => $toBeUpdated1->a,
				'b'          => $toBeUpdated1->b,
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => $toBeUpdated2->a,
				'b'          => $toBeUpdated2->b,
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_customMatchByFieldsWithProcessor_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1 = factory(TestModel::class)->create([
				'a' => '-a1',
				'b' => 'b1-',
			]);
			$toBeUpdated2 = factory(TestModel::class)->create([
				'a' => '-a2',
				'b' => 'b2-',
			]);
			$toBeUnmodified = factory(TestModel::class)->create();

			$dataUpdate1 = [
				'a'  => '+a1',
				'b'  => 'b1+',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2 = [
				'a' => '+a2',
				'b' => 'b2+',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($dataUpdate1, $dataUpdate2) {
						$actualFieldCMap = [];
						foreach ($value as $curr) {
							$actualFieldCMap[$curr['c']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['c'], $actualFieldCMap);
						$this->assertArrayHasKey($dataUpdate2['c'], $actualFieldCMap);
						$this->assertCount(2, $actualFieldCMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['c', 'd']));
			$this->assertSame($import, $import->matchBy([
				'a' => function ($v) {
					return substr($v, 1);
				},
				'b' => function ($v) {
					return substr($v, 0, -1);
				}
			]));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => $toBeUpdated1->a,
				'b'          => $toBeUpdated1->b,
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => $toBeUpdated2->a,
				'b'          => $toBeUpdated2->b,
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_matchByFieldsContainNull() {


			$model = new TestModel();

			$notToBeUpdated1 = factory(TestModel::class)->create([
				'b' => null,
			]);
			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$testModelNoUpdate1 = new TestModel([
				'a'  => $notToBeUpdated1->a,
				'b'  => null,
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate1 = new TestModel([
				'a'  => $toBeUpdated1->a,
				'b'  => $toBeUpdated1->b,
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelNoUpdate1,
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($testModelUpdate1) {
						$actualFieldCMap = [];
						foreach ($value as $curr) {
							$actualFieldCMap[$curr->c] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->c, $actualFieldCMap);
						$this->assertCount(1, $actualFieldCMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));
			$this->assertSame($import, $import->matchBy(['a', 'b']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelNoUpdate1,
				$testModelUpdate1,
				$testModelToInsert,
			]));


			$this->assertDatabaseHas('test_table', $notToBeUpdated1->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => $testModelNoUpdate1->a,
				'b'          => $testModelNoUpdate1->b,
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => $toBeUpdated1->a,
				'b'          => $toBeUpdated1->b,
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_matchByFieldsContainNull_bypassModel() {


			$model = new TestModel();

			$notToBeUpdated1 = factory(TestModel::class)->create([
				'b' => null,
			]);
			$toBeUpdated1 = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$dataUpdate1 = [
				'a'  => $notToBeUpdated1->a,
				'b'  => null,
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2 = [
				'a'  => $toBeUpdated1->a,
				'b'  => $toBeUpdated1->b,
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a'  => 'a4',
				'b'  => 'b4',
				'c'  => 'd4',
				'd'  => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataUpdate1,
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function($value) use ($dataUpdate2) {
						$actualFieldCMap = [];
						foreach ($value as $curr) {
							$actualFieldCMap[$curr['c']] = true;
						}

						$this->assertArrayHasKey($dataUpdate2['c'], $actualFieldCMap);
						$this->assertCount(1, $actualFieldCMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));
			$this->assertSame($import, $import->matchBy(['a', 'b']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
			]));


			$this->assertDatabaseHas('test_table', $notToBeUpdated1->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => $dataUpdate1['a'],
				'b'          => $dataUpdate1['b'],
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => $toBeUpdated1->a,
				'b'          => $toBeUpdated1->b,
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}


		public function testBatchImport_insertAndUpdate_mustBeTypeSafe() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create([
				'a' => '09'
			]);
			$toBeUpdated2   = factory(TestModel::class)->create([
				'a' => '9'
			]);
			$toBeUnmodified = factory(TestModel::class)->create();

			$testModelUpdate1  = new TestModel([
				'a' => $toBeUpdated1->a,
				'b' => $toBeUpdated1->b,
				'c' => 'd1',
				'd' => Carbon::now(),
			]);
			$testModelUpdate2  = new TestModel([
				'a' => $toBeUpdated2->a,
				'b' => $toBeUpdated2->b,
				'c' => 'd2',
				'd' => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualFieldCMap = [];
						foreach ($value as $curr) {
							$actualFieldCMap[$curr->c] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->c, $actualFieldCMap);
						$this->assertArrayHasKey($testModelUpdate2->c, $actualFieldCMap);
						$this->assertCount(2, $actualFieldCMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));
			$this->assertSame($import, $import->matchBy(['a', 'b']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => $toBeUpdated1->a,
				'b'          => $toBeUpdated1->b,
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => $toBeUpdated2->a,
				'b'          => $toBeUpdated2->b,
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_mustBeTypeSafe_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create([
				'a' => '09'
			]);
			$toBeUpdated2   = factory(TestModel::class)->create([
				'a' => '9'
			]);
			$toBeUnmodified = factory(TestModel::class)->create();

			$dataUpdate1  = [
				'a' => $toBeUpdated1->a,
				'b' => $toBeUpdated1->b,
				'c' => 'd1',
				'd' => Carbon::now(),
			];
			$dataUpdate2 = [
				'a' => $toBeUpdated2->a,
				'b' => $toBeUpdated2->b,
				'c' => 'd2',
				'd' => Carbon::now(),
			];
			$dataInsert = [
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($dataUpdate1, $dataUpdate2) {
						$actualFieldCMap = [];
						foreach ($value as $curr) {
							$actualFieldCMap[$curr['c']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['c'], $actualFieldCMap);
						$this->assertArrayHasKey($dataUpdate2['c'], $actualFieldCMap);
						$this->assertCount(2, $actualFieldCMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));
			$this->assertSame($import, $import->matchBy(['a', 'b']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => $toBeUpdated1->a,
				'b'          => $toBeUpdated1->b,
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => $toBeUpdated2->a,
				'b'          => $toBeUpdated2->b,
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withBatchId() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create();
			$toBeUpdated2   = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();
			$toUpdateBatchIdOnly = factory(TestModel::class)->create();

			$testModelUpdate1  = new TestModel([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2  = new TestModel([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			]);

			$testToUpdateBatchIdOnly = new TestModel($toUpdateBatchIdOnly->getAttributes());


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->withBatchId(19));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
				$testToUpdateBatchIdOnly,
			], $lastBatchId));

			$this->assertSame('19', $lastBatchId);
			$this->assertSame('19', $import->getLastBatchId());

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'last_batch_id' => 19,
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'last_batch_id' => 19,
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', array_merge(
				$toUpdateBatchIdOnly->getAttributes(),
				[
					'last_batch_id' => 19
				]
			));

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'last_batch_id' => 19,
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withBatchId_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create();
			$toBeUpdated2   = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();
			$toUpdateBatchIdOnly = factory(TestModel::class)->create();

			$dataUpdate1  = [
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2  = [
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			];

			$testToUpdateBatchIdOnly = $toUpdateBatchIdOnly->getAttributes();


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($dataUpdate1, $dataUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['id'], $actualIdMap);
						$this->assertArrayHasKey($dataUpdate2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->withBatchId(19));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
				$testToUpdateBatchIdOnly,
			], $lastBatchId));

			$this->assertSame('19', $lastBatchId);
			$this->assertSame('19', $import->getLastBatchId());

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'last_batch_id' => 19,
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'last_batch_id' => 19,
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', array_merge(
				$toUpdateBatchIdOnly->getAttributes(),
				[
					'last_batch_id' => 19
				]
			));

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'last_batch_id' => 19,
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withBatchId_withCustomBatchField() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create();
			$toBeUpdated2   = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();
			$toUpdateBatchIdOnly = factory(TestModel::class)->create();

			$testModelUpdate1  = new TestModel([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2  = new TestModel([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			]);

			$testToUpdateBatchIdOnly = new TestModel($toUpdateBatchIdOnly->getAttributes());


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->withBatchId(19, 'c'));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
				$testToUpdateBatchIdOnly,
			], $lastBatchId));

			$this->assertSame('19', $lastBatchId);
			$this->assertSame('19', $import->getLastBatchId());

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => '19',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => '19',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', array_merge(
				$toUpdateBatchIdOnly->getAttributes(),
				[
					'c' => '19'
				]
			));

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => '19',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withBatchId_withCustomBatchField_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create();
			$toBeUpdated2   = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();
			$toUpdateBatchIdOnly = factory(TestModel::class)->create();

			$dataUpdate1  = [
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2  = [
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			];

			$testToUpdateBatchIdOnly = $toUpdateBatchIdOnly->getAttributes();


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					], ['c'])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($dataUpdate1, $dataUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['id'], $actualIdMap);
						$this->assertArrayHasKey($dataUpdate2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->withBatchId(19, 'c'));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
				$testToUpdateBatchIdOnly,
			], $lastBatchId));

			$this->assertSame('19', $lastBatchId);
			$this->assertSame('19', $import->getLastBatchId());

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => '19',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => '19',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', array_merge(
				$toUpdateBatchIdOnly->getAttributes(),
				[
					'c' => '19'
				]
			));

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => '19',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}


		public function testBatchImport_insertAndUpdate_withBatchIdFromModel() {


			$model = new TestModelWithBatch();

			$toBeUpdated1        = factory(TestModelWithBatch::class)->create();
			$toBeUpdated2        = factory(TestModelWithBatch::class)->create();
			$toBeUnmodified      = factory(TestModelWithBatch::class)->create();
			$toUpdateBatchIdOnly = factory(TestModelWithBatch::class)->create();

			$testModelUpdate1  = new TestModelWithBatch([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2  = new TestModelWithBatch([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModelWithBatch([
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			]);

			$testToUpdateBatchIdOnly = new TestModelWithBatch($toUpdateBatchIdOnly->getAttributes());


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
				$testToUpdateBatchIdOnly,
			], $lastBatchId));

			$this->assertSame('25', $lastBatchId);
			$this->assertSame('25', $import->getLastBatchId());


			$this->assertDatabaseHas('test_table', [
				'id'            => $toBeUpdated1->id,
				'a'             => 'a1',
				'b'             => 'b1',
				'c'             => '25',
				'last_batch_id' => 0,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'            => $toBeUpdated2->id,
				'a'             => 'a2',
				'b'             => 'b2',
				'c'             => '25',
				'last_batch_id' => 0,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', array_merge(
				$toUpdateBatchIdOnly->getAttributes(),
				[
					'c' => 25
				]
			));

			$this->assertDatabaseHas('test_table', [
				'a'             => 'a4',
				'b'             => 'b4',
				'c'             => '25',
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withBatchIdFromModel_bypassModel() {


			$model = new TestModelWithBatch();

			$toBeUpdated1        = factory(TestModelWithBatch::class)->create();
			$toBeUpdated2        = factory(TestModelWithBatch::class)->create();
			$toBeUnmodified      = factory(TestModelWithBatch::class)->create();
			$toUpdateBatchIdOnly = factory(TestModelWithBatch::class)->create();

			$dataUpdate1  = [
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2  = [
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			];

			$testToUpdateBatchIdOnly = $toUpdateBatchIdOnly->getAttributes();


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					], ['c'])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($dataUpdate1, $dataUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['id'], $actualIdMap);
						$this->assertArrayHasKey($dataUpdate2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
				$testToUpdateBatchIdOnly,
			], $lastBatchId));

			$this->assertSame('25', $lastBatchId);
			$this->assertSame('25', $import->getLastBatchId());


			$this->assertDatabaseHas('test_table', [
				'id'            => $toBeUpdated1->id,
				'a'             => 'a1',
				'b'             => 'b1',
				'c'             => '25',
				'last_batch_id' => 0,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'            => $toBeUpdated2->id,
				'a'             => 'a2',
				'b'             => 'b2',
				'c'             => '25',
				'last_batch_id' => 0,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', array_merge(
				$toUpdateBatchIdOnly->getAttributes(),
				[
					'c' => 25
				]
			));

			$this->assertDatabaseHas('test_table', [
				'a'             => 'a4',
				'b'             => 'b4',
				'c'             => '25',
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withBatchIdFromModel_butOverwritten() {


			$model = new TestModelWithBatch();

			$toBeUpdated1        = factory(TestModelWithBatch::class)->create();
			$toBeUpdated2        = factory(TestModelWithBatch::class)->create();
			$toBeUnmodified      = factory(TestModelWithBatch::class)->create();
			$toUpdateBatchIdOnly = factory(TestModelWithBatch::class)->create();

			$testModelUpdate1  = new TestModelWithBatch([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2  = new TestModelWithBatch([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModelWithBatch([
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			]);

			$testToUpdateBatchIdOnly = new TestModelWithBatch($toUpdateBatchIdOnly->getAttributes());


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->withBatchId('19', 'last_batch_id'));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
				$testToUpdateBatchIdOnly,
			], $lastBatchId));

			$this->assertSame('19', $lastBatchId);
			$this->assertSame('19', $import->getLastBatchId());

			$this->assertDatabaseHas('test_table', [
				'id'            => $toBeUpdated1->id,
				'a'             => 'a1',
				'b'             => 'b1',
				'c'             => 'd1',
				'last_batch_id' => 19,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'            => $toBeUpdated2->id,
				'a'             => 'a2',
				'b'             => 'b2',
				'c'             => 'd2',
				'last_batch_id' => 19,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', array_merge(
				$toUpdateBatchIdOnly->getAttributes(),
				[
					'last_batch_id' => 19
				]
			));

			$this->assertDatabaseHas('test_table', [
				'a'             => 'a4',
				'b'             => 'b4',
				'c'             => 'd4',
				'last_batch_id' => '19',
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withClosure() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create();
			$toBeUpdated2   = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$testModelUpdate1  = new TestModel([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2  = new TestModel([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2, $testModelToInsert) {
						$actualCMap = [];
						foreach ($value as $curr) {
							$actualCMap[$curr->c] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->c, $actualCMap);
						$this->assertArrayHasKey($testModelUpdate2->c, $actualCMap);
						$this->assertArrayHasKey($testModelToInsert->c, $actualCMap);
						$this->assertCount(3, $actualCMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import(function() use ($testModelUpdate1, $testModelUpdate2, $testModelToInsert) {
				return [
					$testModelUpdate1,
					$testModelUpdate2,
					$testModelToInsert,
				];
			}));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withClosure_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create();
			$toBeUpdated2   = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$dataUpdate1  = [
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2  = [
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($dataUpdate1, $dataUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['id'], $actualIdMap);
						$this->assertArrayHasKey($dataUpdate2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($dataUpdate1, $dataUpdate2, $dataInsert) {
						$actualCMap = [];
						foreach ($value as $curr) {
							$actualCMap[$curr['c']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['c'], $actualCMap);
						$this->assertArrayHasKey($dataUpdate2['c'], $actualCMap);
						$this->assertArrayHasKey($dataInsert['c'], $actualCMap);
						$this->assertCount(3, $actualCMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import(function() use ($dataUpdate1, $dataUpdate2, $dataInsert) {
				return [
					$dataUpdate1,
					$dataUpdate2,
					$dataInsert,
				];
			}));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withSetMutator() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create();
			$toBeUpdated2   = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$testModelUpdate1  = new TestModelWithSetMutator([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2  = new TestModelWithSetMutator([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModelWithSetMutator([
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2, $testModelToInsert) {
						$actualCMap = [];
						foreach ($value as $curr) {
							$actualCMap[$curr->c] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->c, $actualCMap);
						$this->assertArrayHasKey($testModelUpdate2->c, $actualCMap);
						$this->assertArrayHasKey($testModelToInsert->c, $actualCMap);
						$this->assertCount(3, $actualCMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$this->assertSame($import, $import->import([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'mutated:a1',
				'b'          => 'B1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'mutated:a2',
				'b'          => 'B2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'mutated:a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withSetMutator_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create();
			$toBeUpdated2   = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();

			$dataUpdate1  = [
				'id' => $toBeUpdated1->id,
				'a'  => 'mutated:a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2  =[
				'id' => $toBeUpdated2->id,
				'a'  => 'mutated:a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a' => 'mutated:a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			];


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($dataUpdate1, $dataUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['id'], $actualIdMap);
						$this->assertArrayHasKey($dataUpdate2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($dataUpdate1, $dataUpdate2, $dataInsert) {
						$actualCMap = [];
						foreach ($value as $curr) {
							$actualCMap[$curr['c']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['c'], $actualCMap);
						$this->assertArrayHasKey($dataUpdate2['c'], $actualCMap);
						$this->assertArrayHasKey($dataInsert['c'], $actualCMap);
						$this->assertCount(3, $actualCMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel());

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'mutated:a1',
				'b'          => 'B1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'mutated:a2',
				'b'          => 'B2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'mutated:a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(4, DB::table('test_table')->count());
		}

		public function testBatchImport_insertAndUpdate_withSetMutatorAndRawComparator_bypassModel() {


			$model = new TestModel();

			$toBeUpdated1   = factory(TestModel::class)->create();
			$toBeUpdated2   = factory(TestModel::class)->create();
			$toBeUnmodified = factory(TestModel::class)->create();
			$toBeUnmodifiedDueToComparator = factory(TestModel::class)->create([
				'a' => 'mutated:a5'
			]);

			$dataUpdate1  = [
				'id' => $toBeUpdated1->id,
				'a'  => 'mutated:a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			];
			$dataUpdate2  =[
				'id' => $toBeUpdated2->id,
				'a'  => 'mutated:a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			];
			$dataInsert = [
				'a' => 'mutated:a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			];

			$dataUpdateNotToModify = array_merge($toBeUnmodifiedDueToComparator->toArray(), ['a' => 'a5']);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					$this->itemsSubsetMatchesCallback([
						$dataInsert,
					])
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($dataUpdate1, $dataUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr['id']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['id'], $actualIdMap);
						$this->assertArrayHasKey($dataUpdate2['id'], $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($dataUpdate1, $dataUpdate2, $dataInsert) {
						$actualCMap = [];
						foreach ($value as $curr) {
							$actualCMap[$curr['c']] = true;
						}

						$this->assertArrayHasKey($dataUpdate1['c'], $actualCMap);
						$this->assertArrayHasKey($dataUpdate2['c'], $actualCMap);
						$this->assertArrayHasKey($dataInsert['c'], $actualCMap);
						$this->assertCount(3, $actualCMap);

						return true;
					})
				],
				'insertOrUpdateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());

			$this->assertSame($import, $import->bypassModel(true, [
				'a' => function($new, $current) {
					return str_replace('mutated:', '', $new) != str_replace('mutated:', '', $current);
				}
			]));

			$this->assertSame($import, $import->import([
				$dataUpdate1,
				$dataUpdate2,
				$dataInsert,
				$dataUpdateNotToModify,
			]));


			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated1->id,
				'a'          => 'mutated:a1',
				'b'          => 'B1',
				'c'          => 'd1',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'         => $toBeUpdated2->id,
				'a'          => 'mutated:a2',
				'b'          => 'B2',
				'c'          => 'd2',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', [
				'a'          => 'mutated:a4',
				'b'          => 'b4',
				'c'          => 'd4',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodifiedDueToComparator->getAttributes());

			$this->assertSame(5, DB::table('test_table')->count());
		}

		public function testConstructor_modelWithoutPrimaryKey() {

			$this->expectException(InvalidArgumentException::class);

			new BatchImport(new TestModelWithoutPrimaryKey());

		}

		public function testMatchByFields_empty() {

			$import = new BatchImport(new TestModel());

			$this->expectException(InvalidArgumentException::class);

			$import->matchBy([]);

		}

		public function testMatchByFields_noCallableAssocItem() {

			$import = new BatchImport(new TestModel());

			$this->expectException(InvalidArgumentException::class);

			$import->matchBy(['a' => 15]);

		}

		public function testMatchByFields_emptyArrayKey() {

			$import = new BatchImport(new TestModel());

			$this->expectException(InvalidArgumentException::class);

			$import->matchBy(['' => function() {}]);

		}

		public function testUpdateIfExists_emptyArrayKey() {
			$import = new BatchImport(new TestModel());

			$this->expectException(InvalidArgumentException::class);

			$import->updateIfExists(['' => 15]);
		}

		public function testBufferSize() {
			$import = new BatchImport(new TestModel());

			// we simply check for no error
			$this->assertSame($import, $import->buffer(800, 100));
		}

		public function testBufferSize_callbackBufferSameSize() {
			$import = new BatchImport(new TestModel());

			// we simply check for no error
			$this->assertSame($import, $import->buffer(800));
		}

		public function testBatchImport_prepare_insertOnly() {


			$model = new TestModel();

			$testModel1 = new TestModel([
				'a' => 'a1',
				'b' => 'b1',
				'c' => 'd1',
				'd' => Carbon::now(),
			]);
			$testModel2 = new TestModel([
				'a' => 'a2',
				'b' => 'b2',
				'c' => 'd2',
				'd' => Carbon::now(),
			]);


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModel1,
						$testModel2,
					],
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(0, [], 'updateCallback'));
			$import->onInsertedOrUpdated($this->expectedCallback(
				1,
				[
					[
						$testModel1,
						$testModel2,
					],
				],
				'insertOrUpdateCallback'
			));


			$prepared = $import->prepare();

			$prepared->add($testModel1);
			$prepared->add($testModel2);

			$prepared->flush();

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a1',
				'b'          => 'b1',
				'c'          => 'd1',
				'd'          => Carbon::now(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a2',
				'b'          => 'b2',
				'c'          => 'd2',
				'd'          => Carbon::now(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(2, DB::table('test_table')->count());
		}

		public function testBatchImport_prepare_insertAndUpdate_withBatchId() {


			$model = new TestModel();

			$toBeUpdated1        = factory(TestModel::class)->create();
			$toBeUpdated2        = factory(TestModel::class)->create();
			$toBeUnmodified      = factory(TestModel::class)->create();
			$toUpdateBatchIdOnly = factory(TestModel::class)->create();

			$testModelUpdate1  = new TestModel([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2  = new TestModel([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModel([
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			]);

			$testToUpdateBatchIdOnly = new TestModel($toUpdateBatchIdOnly->getAttributes());


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->withBatchId(19));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$prepared = $import->prepare();

			$prepared->addMultiple([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
				$testToUpdateBatchIdOnly,
			]);

			$this->assertSame($prepared, $prepared->flush($lastBatchId));

			$this->assertSame('19', $lastBatchId);
			$this->assertSame('19', $import->getLastBatchId());

			$this->assertDatabaseHas('test_table', [
				'id'            => $toBeUpdated1->id,
				'a'             => 'a1',
				'b'             => 'b1',
				'c'             => 'd1',
				'last_batch_id' => 19,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'            => $toBeUpdated2->id,
				'a'             => 'a2',
				'b'             => 'b2',
				'c'             => 'd2',
				'last_batch_id' => 19,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', array_merge(
				$toUpdateBatchIdOnly->getAttributes(),
				[
					'last_batch_id' => 19
				]
			));

			$this->assertDatabaseHas('test_table', [
				'a'             => 'a4',
				'b'             => 'b4',
				'c'             => 'd4',
				'last_batch_id' => 19,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}

		public function testBatchImport_prepare_insertAndUpdate_withBatchIdFromModel() {


			$model = new TestModelWithBatch();

			$toBeUpdated1        = factory(TestModelWithBatch::class)->create();
			$toBeUpdated2        = factory(TestModelWithBatch::class)->create();
			$toBeUnmodified      = factory(TestModelWithBatch::class)->create();
			$toUpdateBatchIdOnly = factory(TestModelWithBatch::class)->create();

			$testModelUpdate1  = new TestModelWithBatch([
				'id' => $toBeUpdated1->id,
				'a'  => 'a1',
				'b'  => 'b1',
				'c'  => 'd1',
				'd'  => Carbon::now(),
			]);
			$testModelUpdate2  = new TestModelWithBatch([
				'id' => $toBeUpdated2->id,
				'a'  => 'a2',
				'b'  => 'b2',
				'c'  => 'd2',
				'd'  => Carbon::now(),
			]);
			$testModelToInsert = new TestModelWithBatch([
				'a' => 'a4',
				'b' => 'b4',
				'c' => 'd4',
				'd' => Carbon::now(),
			]);

			$testToUpdateBatchIdOnly = new TestModelWithBatch($toUpdateBatchIdOnly->getAttributes());


			$import = new BatchImport($model);

			$import->onInserted($this->expectedCallback(
				1,
				[
					[
						$testModelToInsert,
					]
				],
				'insertCallback'
			));
			$import->onUpdated($this->expectedCallback(
				1,
				[
					$this->callback(function ($value) use ($testModelUpdate1, $testModelUpdate2) {
						$actualIdMap = [];
						foreach ($value as $curr) {
							$actualIdMap[$curr->id] = true;
						}

						$this->assertArrayHasKey($testModelUpdate1->id, $actualIdMap);
						$this->assertArrayHasKey($testModelUpdate2->id, $actualIdMap);
						$this->assertCount(2, $actualIdMap);

						return true;
					})
				],
				'updateCallback'
			));

			$this->assertSame($import, $import->updateIfExists(['a', 'b', 'c', 'd']));

			// shift time
			Carbon::setTestNow(Carbon::now()->addHour());


			$prepared = $import->prepare();

			$prepared->addMultiple([
				$testModelUpdate1,
				$testModelUpdate2,
				$testModelToInsert,
				$testToUpdateBatchIdOnly,
			]);

			$this->assertSame($prepared, $prepared->flush($lastBatchId));

			$this->assertSame('25', $lastBatchId);
			$this->assertSame('25', $import->getLastBatchId());


			$this->assertDatabaseHas('test_table', [
				'id'            => $toBeUpdated1->id,
				'a'             => 'a1',
				'b'             => 'b1',
				'c'             => '25',
				'last_batch_id' => 0,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', [
				'id'            => $toBeUpdated2->id,
				'a'             => 'a2',
				'b'             => 'b2',
				'c'             => '25',
				'last_batch_id' => 0,
				'd'             => Carbon::now()->subHour(),
				'updated_at'    => Carbon::now(),
				'created_at'    => Carbon::now()->subHour(),
			]);

			$this->assertDatabaseHas('test_table', $toBeUnmodified->getAttributes());

			$this->assertDatabaseHas('test_table', array_merge(
				$toUpdateBatchIdOnly->getAttributes(),
				[
					'c' => 25
				]
			));

			$this->assertDatabaseHas('test_table', [
				'a'          => 'a4',
				'b'          => 'b4',
				'c'          => '25',
				'd'          => Carbon::now()->subHour(),
				'updated_at' => Carbon::now(),
				'created_at' => Carbon::now(),
			]);

			$this->assertSame(5, DB::table('test_table')->count());
		}
	}