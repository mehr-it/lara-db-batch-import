<?php


	namespace MehrItLaraDbBatchImportTest\Cases\Unit\Model;


	use MehrIt\LaraDbBatchImport\BatchImport;
	use MehrItLaraDbBatchImportTest\Cases\TestCase;
	use MehrItLaraDbBatchImportTest\Model\TestModelProvidingBatchImport;

	class ProvidesBatchImportTest extends TestCase
	{

		public function testBatchImport() {

			$mock = $this->getMockBuilder(BatchImport::class)->disableOriginalConstructor()->getMock();

			app()->bind(BatchImport::class, function($a, $params) use ($mock) {

				$this->assertInstanceOf(TestModelProvidingBatchImport::class, $params['model']);

				return $mock;
			});


			$this->assertSame($mock, TestModelProvidingBatchImport::batchImport());
		}

		public function testBatchImport_integration() {

			$this->assertSame(BatchImport::class, get_class(TestModelProvidingBatchImport::batchImport()));
		}

	}