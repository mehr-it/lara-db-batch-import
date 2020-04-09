<?php

	// @formatter:off

	/**
	 * A helper file for Laravel to provide autocomplete information to your IDE
	 *
	 * This file should not be included in your code, only analyzed by your IDE!
	 */

	namespace Illuminate\Database\Eloquent {

		class Builder
		{

			/**
			 * Adds a where condition to match only records which have been missing since the given batch
			 * @param string|null $batchId The batch id
			 * @param string|null $batchIdField The batch if field if it should be set manually
			 * @return \Illuminate\Database\Eloquent\Builder
			 */
			function whereMissingAfterBatch(string $batchId, string $batchIdField = null) {

			}


		}
	}
