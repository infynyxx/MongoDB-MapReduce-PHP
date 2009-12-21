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
						emit(x, {count: 1});
					}
				);
			}
MAP;

		$this->reduce = <<<REDUCE
			function(k, v)  {
		        var total = 0;
		        for (var i = 0; i < v.length; i++)  {
		            total += v[i].count;
		        }
		        return {count: total};
		    }
REDUCE;
	}
	
	public function testInvalidDBNameShouldReturnNSDoesNotExist()	{
		$this->dbName = "22test_dbsaaa";
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
	
	
	public function testResultSet()	{
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		
		$mr_real_keys = Array();
		$mr_expected_keys = Array("dog", "cat", "mouse");
		
		foreach ($response->getResultSet() as $values)	{
			array_push($mr_real_keys, $values["_id"]);
			$this->assertTrue(in_array($values["_id"], $mr_expected_keys));
		}
		
	}
	
	public function testCountData()	{
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		if ($response->valid())	{
			$count_data = $response->getCountsData();
			$count_data_keys = array_keys($count_data);
			$real = array_diff($count_data_keys, Array("input", "output", "emit"));
			$expected = Array();
			$this->assertEquals($expected, $real);
		}
		
	}
	
	public function testExecutionTime()	{
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		$this->assertTrue(is_numeric($response->getTotalExecutionTime()));
	}
	
	
	public function testGlobalScope()	{
		$map_reduce = new MongoMapReduce($this->map, $this->reduce);
		$global_value = 'this is global value!';
		$scope = Array('global_value' => $global_value);
		$finalize = <<<FINALIZE
			function finalize(k, v)	{
				return {count: v.count, global: global_value};
			}
		
FINALIZE;
		$map_reduce->setScope($scope);
		$map_reduce->setFinalize($finalize);
		$response = $map_reduce->invoke($this->mongoDB, $this->collectionName);
		
		foreach ($response->getResultSet() as $cursor)	{
			$this->assertEquals($global_value, $cursor["value"]["global"]);
		}
	}
}