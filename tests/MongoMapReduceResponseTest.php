<?php

require_once "_include.php";
	
class MongoMapReduceResponseTest extends PHPUnit_Framework_TestCase	{
	
	private $dbName;
	private $mapReduce;
	private $mongoDB;
	
	private $collectionName;
	
	private $map;
	
	private $reduce;

	public function setUp()	{
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
	
	public function testInvalidDBNameShouldReturnNSDoesNotExist()	{
		$this->dbName = "test_dbsaaa";
		$this->mongoDB = new MongoDB(new Mongo(), $this->dbName);
		
		$this->collectionName = "animal_tags";
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		
		$error_msg = $response->getErrorMessage();
		
		$this->assertEquals($error_msg, "ns doesn't exist");
	}
	
	public function testInvalidCollectionShouldReturnNSDoesNotExist()	{
		$this->mongoDB = new MongoDB(new Mongo(), $this->dbName);
		
		$this->collectionName = "animal_tagsaaa";	//invalid collection
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		
		$error_msg = $response->getErrorMessage();
		
		$this->assertEquals($error_msg, "ns doesn't exist");
	}
	
	public function testResponseIsOKWhenMapReduceQueryIsValid()	{
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		$this->assertTrue($response->valid());
	}
	
	public function testResponseIsFalseWhenMapFunctioIsInvalid()	{
		$this->map =  <<<MAP
			function()	{
				his.tags.forEach(	//should have been *this.tags.forEach*
					function(x)	{
						emit(x, 1);
					}
				);
			}
MAP;
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		$this->assertFalse($response->valid());
	}
	
	
	public function testReponseIsFalseWhenReduceFunctionIsInvalid()	{
		$this->reduce = <<<REDUCE
			unction(key, values)	{	//should have been *function(key, values)*
				return {count: values.lengtH };
			}
REDUCE;
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		$this->assertFalse($response->valid());
	}
	
	public function testResultSet()	{
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		
		$mr_real_keys = Array();
		$mr_expected_keys = Array("dog", "cat", "mouse");
		
		foreach ($response->getResultSet() as $values)	{
			array_push($mr_real_keys, $values["_id"]);
			$this->assertTrue(in_array($values["_id"], $mr_expected_keys));
		}
		
		$expected = Array();
		$real = array_diff($mr_expected_keys, $mr_real_keys);
		$this->assertEquals($expected, $real);
		
	}
	
	public function testCountData()	{
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		$count_data = array_keys($response->getCountsData());
		$real = array_diff($count_data, Array("input", "output", "emit"));
		$expected = Array();
		$this->assertEquals($expected, $real);
	}
	
	public function testExecutionTime()	{
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		$this->assertTrue(is_integer($response->getTotalExecutionTime()));
	}
	
}