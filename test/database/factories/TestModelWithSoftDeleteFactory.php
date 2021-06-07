<?php

	use Faker\Generator as Faker;

	/** @var \Illuminate\Database\Eloquent\Factory $factory */
	$factory->define(\MehrItLaraDbBatchImportTest\Model\TestModelWithSoftDelete::class, function (Faker $faker) {
		return [
			'a'             => $faker->text(),
			'b'             => $faker->text(),
			'c'             => $faker->text(),
			'last_batch_id' => 0,
		];
	});
	
	$factory->state(\MehrItLaraDbBatchImportTest\Model\TestModelWithSoftDelete::class, 'deleted', function() {
		
		return [
			'deleted_at' => \Carbon\Carbon::now()
		];
	});