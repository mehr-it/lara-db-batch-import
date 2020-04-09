<?php

	use Illuminate\Database\Migrations\Migration;
	use Illuminate\Database\Schema\Blueprint;
	use Illuminate\Support\Facades\Schema;

	/**
	 * Created by PhpStorm.
	 * User: chris
	 * Date: 28.11.18
	 * Time: 14:14
	 */
	class CreateTestTable extends Migration
	{
		/**
		 * Run the migrations.
		 *
		 * @return void
		 */
		public function up() {

			Schema::create('test_table', function (Blueprint $table) {
				$table->bigIncrements('id');
				$table->string('a', 255);
				$table->string('b', 255)->nullable();
				$table->string('c', 255)->nullable();
				$table->dateTime('d')->nullable();
				$table->bigInteger('last_batch_id')->nullable();
				$table->timestamps();
			});
		}

		/**
		 * Reverse the migrations.
		 *
		 * @return void
		 */
		public function down() {
			Schema::dropIfExists('test_table');
		}
	}