<?php

	use Faker\Generator as Faker;

	/** @var \Illuminate\Database\Eloquent\Factory $factory */
	$factory->define(\MehrItLaraDbBatchImportTest\Model\TestModelWithBatch::class, function (Faker $faker) {
		return [
			'a'             => $faker->asciify('****************'),
			'b'             => $faker->asciify('****************'),
			'c'             => $faker->asciify('****************'),
			'last_batch_id' => 0,
		];
	});