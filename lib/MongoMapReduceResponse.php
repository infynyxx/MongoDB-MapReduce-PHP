<?php

/**
 * (c) Prajwal Tuladhar <praj@prajwal-tuladhar.net.np>
 * 
 * This class takes handles MapReduce Reponse
 *
 */
class MongoMapReduceResponse	{
	
	/**
	 * @var MongoMapReduce
	 */
	private $_response;
	
	/**
	 * @var MongoDB
	 */
	private $mongoDB;
	
	/**
	 * @var MongoCollection
	 */
	private $collection;
	
	
	public function __construct(MongoDB $mongoDB, Array $mapReduce)	{
		$this->_response = $mapReduce;
		$this->mongoDB = $mongoDB;
	}
	
	
	/**
	 * MapReduce Result Set
	 * @return MongoCursor
	 */
	public function getResultSet(Array $query =  Array())	{
		if (!isset($this->collection))	{
			$this->collection = new MongoCollection($this->mongoDB, $this->_response["result"]);
		}
		return $this->collection->find($query);
	}
	
	
	/**
	 * check if the MR operation failed or not
	 * recommended to use this method before using MR result set
	 * @return boolean
	 */
	public function valid()	{
		return $this->_response["ok"] == 1 ? TRUE : FALSE;
	}
	
	
	/**
	 * Get Number of Objects being iterated in MapReduce Query
	 * @return integer
	 */
	public function getNumberOfObjects()	{
		return $this->_response['numObjects'];
	}
	
	
	/**
	 * Get total time in Milli Seconds to execute the MapReduce operation
	 * @return double
	 */
	public function getTotalExecutionTime()	{
		return $this->_response['timeMillis'];
	}
	
		
	/**
	 * MapReduce Raw Response with index fields: timeMillis, numObjects, timeMillis.emi, ok, result
	 * @return Array
	 */
	public function getRawResponse()	{
		return $this->_response;
	}
	
	
	/**
	 * error message of the MR operation
	 * @return String
	 */
	public function getErrorMessage()	{
		return $this->_response['errmsg'];
	}
	
	
	/**
	 * get Array with keys: input, emit, output
	 * input: Total number of collections that are iterated over
	 * emit: Total number of emit functions being called
	 * output: Total number of output keys
	 * @return Array
	 */
	public function getCountsData()	{
		return $this->_response['counts'];
	}
	
	
	/**
	 * Drop the collection created by MapReduce
	 * @return Array
	 */
	public function dropResultSet()	{
		if (!isset($this->collection))	{
			$this->collection = new MongoCollection($this->mongoDB, $this->_response["result"]);
		}
		$db_response = $this->collection->drop();
		$this->collection = NULL;
		return $db_response;
		
	}
}
