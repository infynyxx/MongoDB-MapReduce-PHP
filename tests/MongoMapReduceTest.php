<?php

require_once 'PHPUnit/Framework.php';
require_once 'lib/MongoMapReduce.php';
	
class MongoMapReduceTest extends PHPUnit_Framework_TestCase	{
	
	private $dbName;
	private $mapReduce;
	private $mongoDB;
	
	private $collectionName;
	
	private $map;
	
	private $reduce;
	
	protected function setUp()	{
		$this->dbName = "test_dbs";
		$this->mongoDB = new MongoDB(new Mongo(), $this->dbName);
		
		$this->collectionName = "animal_tags";
		
		$this->map =  <<<MAP
			function()	{
				this.tags.forEach(
					function(x)	{
						emit(x, 1);
					}
				);
			}
MAP;

		$this->reduce = <<<REDUCE
			function(key, values)	{
				return {count: values.length };
			}
REDUCE;

		

	}
	
	public function testShouldReturnMonggoMapReduceResponseObjectAfterUsingInvokeMethod()	{
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$this->assertTrue($map_reduce->invoke($this->mongoDB, "animal_tags") instanceof MongoMapReduceResponse);
	}
	
}	