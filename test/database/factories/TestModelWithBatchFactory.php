<?php

	use Faker\Generator as Faker;

	/** @var \Illuminate\Database\Eloquent\Factory $factory */
	$factory->define(\MehrItLaraDbBatchImportTest\Model\TestModelWithBatch::class, function (Faker $faker) {
		return [
			'a'             => $faker->text(),
			'b'             => $faker->text(),
			'c'             => $faker->text(),
			'last_batch_id' => 0,
		];
	});