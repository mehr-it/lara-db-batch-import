<?php


	namespace MehrIt\LaraDbBatchImport\Contracts;


	interface GeneratesBatchIds
	{

		/**
		 * Gets the next batch id
		 * @return string The next batch id as string
		 */
		public function nextBatchId(): string;

	}