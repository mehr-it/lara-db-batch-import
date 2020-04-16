<?php


	namespace MehrItLaraDbBatchImportTest\Cases\Unit;


	use Illuminate\Database\Eloquent\Model;
	use MehrIt\Buffer\FlushingBuffer;
	use MehrIt\LaraDbBatchImport\BatchImport;
	use MehrIt\LaraDbBatchImport\PreparedBatchImport;
	use MehrItLaraDbBatchImportTest\Cases\TestCase;
	use PHPUnit\Framework\MockObject\MockObject;

	class PreparedBatchImportTest extends TestCase
	{

		public function testAddImport_calledOnce() {

			/** @var Model|MockObject $m1 */
			$m1 = $this->getMockBuilder(Model::class)->getMock();

			$buffer = new FlushingBuffer(500, $this->expectedCallback(1, [[$m1]]));

			$flushAllCallback = function() use ($buffer, $m1) {

				$buffer->flush();

				return 19;
			};

			$pi = new PreparedBatchImport($buffer, $flushAllCallback);

			$this->assertSame($pi, $pi->add($m1));

			$this->assertSame($pi, $pi->flush($lastBatchId));

			$this->assertSame(19, $lastBatchId);
		}

		public function testAddImport_calledMultipleTimes() {

			/** @var Model|MockObject $m1 */
			$m1 = $this->getMockBuilder(Model::class)->getMock();
			/** @var Model|MockObject $m2 */
			$m2 = $this->getMockBuilder(Model::class)->getMock();

			$buffer = new FlushingBuffer(500, $this->expectedCallback(1, [[$m1, $m2]]));

			$flushAllCallback = function () use ($buffer, $m1) {

				$buffer->flush();

				return 19;
			};

			$pi = new PreparedBatchImport($buffer, $flushAllCallback);

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

			$buffer = new FlushingBuffer(500, $this->expectedCallback(1, [[$m1, $m2]]));

			$flushAllCallback = function () use ($buffer, $m1) {

				$buffer->flush();

				return 19;
			};

			$pi = new PreparedBatchImport($buffer, $flushAllCallback);

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

			$buffer = new FlushingBuffer(500, $this->expectedCallback(1, [[$m1, $m2, $m3, $m4]]));

			$flushAllCallback = function () use ($buffer, $m1) {

				$buffer->flush();

				return 19;
			};

			$pi = new PreparedBatchImport($buffer, $flushAllCallback);

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

			$buffer = new FlushingBuffer(500, $this->expectedCallback(1, [[$m1, $m2]]));

			$flushAllCallback = function () use ($buffer, $m1) {

				$buffer->flush();

				return 19;
			};

			$pi = new PreparedBatchImport($buffer, $flushAllCallback);

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

			$buffer = new FlushingBuffer(500, $this->expectedCallback(1, [[$m1, $m2, $m3, $m4]]));

			$flushAllCallback = function () use ($buffer, $m1) {

				$buffer->flush();

				return 19;
			};

			$pi = new PreparedBatchImport($buffer, $flushAllCallback);

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

			$buffer = new FlushingBuffer(500, $this->expectedCallback(1, [[$m1, $m2, $m3, $m4, $m5]]));

			$flushAllCallback = function () use ($buffer, $m1) {

				$buffer->flush();

				return 19;
			};

			$pi = new PreparedBatchImport($buffer, $flushAllCallback);

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