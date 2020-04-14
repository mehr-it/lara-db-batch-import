<?php


	namespace MehrItLaraDbBatchImportTest\Cases\Unit;


	use Illuminate\Database\Eloquent\Model;
	use MehrIt\LaraDbBatchImport\BatchImport;
	use MehrIt\LaraDbBatchImport\PreparedBatchImport;
	use MehrItLaraDbBatchImportTest\Cases\TestCase;
	use PHPUnit\Framework\MockObject\MockObject;

	class PreparedBatchImportTest extends TestCase
	{

		public function testAddImport_calledOnce() {

			/** @var Model|MockObject $m1 */
			$m1 = $this->getMockBuilder(Model::class)->getMock();

			/** @var BatchImport|MockObject $batchImportMock */
			$batchImportMock = $this->getMockBuilder(BatchImport::class)->disableOriginalConstructor()->getMock();
			$batchImportMock
				->expects($this->once())
				->method('import')
				->willReturnCallback(function($records, &$lastBatchId = null) use ($batchImportMock, $m1) {

					$this->assertSame(
						[
							$m1,
						],
						iterator_to_array($records)
					);

					$lastBatchId = 19;

					return $batchImportMock;
				});

			$pi = new PreparedBatchImport($batchImportMock);

			$this->assertSame($pi, $pi->add($m1));

			$this->assertSame($pi, $pi->flush($lastBatchId));

			$this->assertSame(19, $lastBatchId);
		}

		public function testAddImport_calledMultipleTimes() {

			/** @var Model|MockObject $m1 */
			$m1 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m2 */
			$m2 = $this->getMockBuilder(Model::class)->getMock();

			/** @var BatchImport|MockObject $batchImportMock */
			$batchImportMock = $this->getMockBuilder(BatchImport::class)->disableOriginalConstructor()->getMock();
			$batchImportMock
				->expects($this->once())
				->method('import')
				->willReturnCallback(function($records, &$lastBatchId = null) use ($batchImportMock, $m1, $m2) {

					$this->assertSame(
						[
							$m1,
							$m2,
						],
						iterator_to_array($records)
					);

					$lastBatchId = 19;

					return $batchImportMock;
				});

			$pi = new PreparedBatchImport($batchImportMock);

			$this->assertSame($pi, $pi->add($m1));
			$this->assertSame($pi, $pi->add($m2));

			$this->assertSame($pi, $pi->flush($lastBatchId));

			$this->assertSame(19, $lastBatchId);
		}

		public function testAddMultiple_calledOnce() {

			/** @var Model|MockObject $m1 */
			$m1 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m2 */
			$m2 = $this->getMockBuilder(Model::class)->getMock();

			/** @var BatchImport|MockObject $batchImportMock */
			$batchImportMock = $this->getMockBuilder(BatchImport::class)->disableOriginalConstructor()->getMock();
			$batchImportMock
				->expects($this->once())
				->method('import')
				->willReturnCallback(function($records, &$lastBatchId = null) use ($batchImportMock, $m1, $m2) {

					$this->assertSame(
						[
							$m1,
							$m2,
						],
						iterator_to_array($records)
					);

					$lastBatchId = 19;

					return $batchImportMock;
				});

			$pi = new PreparedBatchImport($batchImportMock);

			$this->assertSame($pi, $pi->addMultiple([$m1, $m2]));

			$this->assertSame($pi, $pi->flush($lastBatchId));

			$this->assertSame(19, $lastBatchId);
		}

		public function testAddMultiple_calledMultipleItems() {

			/** @var Model|MockObject $m1 */
			$m1 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m2 */
			$m2 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m3 */
			$m3 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m4 */
			$m4 = $this->getMockBuilder(Model::class)->getMock();

			/** @var BatchImport|MockObject $batchImportMock */
			$batchImportMock = $this->getMockBuilder(BatchImport::class)->disableOriginalConstructor()->getMock();
			$batchImportMock
				->expects($this->once())
				->method('import')
				->willReturnCallback(function($records, &$lastBatchId = null) use ($batchImportMock, $m1, $m2, $m3, $m4) {

					$this->assertSame(
						[
							$m1,
							$m2,
							$m3,
							$m4
						],
						iterator_to_array($records)
					);

					$lastBatchId = 19;

					return $batchImportMock;
				});

			$pi = new PreparedBatchImport($batchImportMock);

			$this->assertSame($pi, $pi->addMultiple([$m1, $m2]));
			$this->assertSame($pi, $pi->addMultiple([$m3, $m4]));

			$this->assertSame($pi, $pi->flush($lastBatchId));

			$this->assertSame(19, $lastBatchId);
		}

		public function testAddMultiple_withGenerator_calledOnce() {

			/** @var Model|MockObject $m1 */
			$m1 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m2 */
			$m2 = $this->getMockBuilder(Model::class)->getMock();

			/** @var BatchImport|MockObject $batchImportMock */
			$batchImportMock = $this->getMockBuilder(BatchImport::class)->disableOriginalConstructor()->getMock();
			$batchImportMock
				->expects($this->once())
				->method('import')
				->willReturnCallback(function ($records, &$lastBatchId = null) use ($batchImportMock, $m1, $m2) {

					$this->assertSame(
						[
							$m1,
							$m2,
						],
						iterator_to_array($records)
					);

					$lastBatchId = 19;

					return $batchImportMock;
				});

			$pi = new PreparedBatchImport($batchImportMock);

			$gen1 = function () use ($m1, $m2) {
				yield $m1;
				yield $m2;
			};

			$this->assertSame($pi, $pi->addMultiple($gen1()));

			$this->assertSame($pi, $pi->flush($lastBatchId));

			$this->assertSame(19, $lastBatchId);
		}

		public function testAddMultiple_withGenerator_calledMultipleItems() {

			/** @var Model|MockObject $m1 */
			$m1 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m2 */
			$m2 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m3 */
			$m3 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m4 */
			$m4 = $this->getMockBuilder(Model::class)->getMock();

			/** @var BatchImport|MockObject $batchImportMock */
			$batchImportMock = $this->getMockBuilder(BatchImport::class)->disableOriginalConstructor()->getMock();
			$batchImportMock
				->expects($this->once())
				->method('import')
				->willReturnCallback(function ($records, &$lastBatchId = null) use ($batchImportMock, $m1, $m2, $m3, $m4) {

					$this->assertSame(
						[
							$m1,
							$m2,
							$m3,
							$m4
						],
						iterator_to_array($records)
					);

					$lastBatchId = 19;

					return $batchImportMock;
				});

			$pi = new PreparedBatchImport($batchImportMock);

			$gen1 = function () use ($m1, $m2) {
				yield $m1;
				yield $m2;
			};

			$gen2 = function () use ($m3, $m4) {
				yield $m3;
				yield $m4;
			};

			$this->assertSame($pi, $pi->addMultiple($gen1()));
			$this->assertSame($pi, $pi->addMultiple($gen2()));

			$this->assertSame($pi, $pi->flush($lastBatchId));

			$this->assertSame(19, $lastBatchId);
		}

		public function testAddAndAddMultipleMixed() {

			/** @var Model|MockObject $m1 */
			$m1 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m2 */
			$m2 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m3 */
			$m3 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m4 */
			$m4 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m5 */
			$m5 = $this->getMockBuilder(Model::class)->getMock();

			/** @var BatchImport|MockObject $batchImportMock */
			$batchImportMock = $this->getMockBuilder(BatchImport::class)->disableOriginalConstructor()->getMock();
			$batchImportMock
				->expects($this->once())
				->method('import')
				->willReturnCallback(function ($records, &$lastBatchId = null) use ($batchImportMock, $m1, $m2, $m3, $m4, $m5) {

					$this->assertSame(
						[
							$m1,
							$m2,
							$m3,
							$m4,
							$m5,
						],
						iterator_to_array($records)
					);

					$lastBatchId = 19;

					return $batchImportMock;
				});

			$pi = new PreparedBatchImport($batchImportMock);

			$gen1 = function () use ($m2, $m3) {
				yield $m2;
				yield $m3;
			};

			$this->assertSame($pi, $pi->add($m1));
			$this->assertSame($pi, $pi->addMultiple($gen1()));
			$this->assertSame($pi, $pi->addMultiple([$m4, $m5]));

			$this->assertSame($pi, $pi->flush($lastBatchId));

			$this->assertSame(19, $lastBatchId);
		}

	}