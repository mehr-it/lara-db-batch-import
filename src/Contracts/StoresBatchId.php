<?php


	namespace MehrIt\LaraDbBatchImport\Contracts;


	interface StoresBatchId
	{

		/**
		 * Gets the name of the field to store the batch id
		 * @return string The batch id field name
		 */
		public function getBatchIdField(): string;

	}