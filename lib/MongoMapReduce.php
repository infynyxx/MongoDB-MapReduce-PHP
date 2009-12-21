<?php

/**
 * (c) Prajwal Tuladhar <praj@prajwal-tuladhar.net.np>
 * 
 * This class takes required parameters to perform MongoDB MapReduce 
 *
 */

class MongoMapReduce
{
	/**
	 * MapReduce Query Parameter variable
	 * @var String
	 */
	private $_mapReduce;
	
	/**
	 * MapReduce Response
	 * @var String
	 */
	private $_response;
	
	/**
	 * @var MongoDB
	 */
	private $mongoDB;
		
	
	/**
	 * Name of the collection
	 * @var String
	 */
	private $collectionNamespace;
	
	
	/**
	 * Map Code
	 * @var String
	 */
	private $map;
	
	
	/**
	 * Reduce Code
	 * @var String
	 */
	private $reduce;
	
	
	/**
	 * Query Param
	 * @var Array
	 */
	private $query = Array();
	
	
	/**
	 * Name of the ouput collection
	 * @var String
	 */
	private $outputCollection;
	
	
	/**
	 * Flag indicating whether to keep temporary collection created from MapReduce
	 * @var unknown_type
	 */
	private $keepTemporaryCollection;
	
	private $finalize;
	
	private $limit;
	
	private $sort = Array();
	
	private $scope = Array();
	
	const VALUE = 'value';	//value index for MR Result Set
	
	const COUNT = 'count';	//number of objects in a MR Result Set
	
	const KEYS_MAPREDUCE = "mapreduce";
	const KEYS_MAP = "map";
	const KEYS_REDUCE = "reduce";
	const KEYS_QUERY = "query";
	const KEYS_KEEP_TEMPORARY_COLLECTION = "keeptemp";
	const KEYS_FINALIZE = "finalize";
	const KEYS_OUTPUT_COLLECTION = "out";
	const KEYS_OUTPUT_SORT = "sort";
	const KEYS_OUTPUT_LIMIT = "limit";
	const KEYS_OUTPUT_SCOPE = "scope";
	
	
	/**
	 * 
	 * @param String $map
	 * @param String $reduce
	 * @param Array $query
	 * @param String $outputCollection
	 * @param Boolean $keepTemporaryCollection
	 * @param String $finalize
	 * @param Int $limit
	 * @param Array $sort
	 */
	public function __construct($map, 
								$reduce, 
								Array $query = Array(), 
								$outputCollection = NULL, 
								$keepTemporaryCollection = FALSE, 
								$finalize = NULL,
								$limit = NULL,
								Array $sort = Array(),
								Array $scope = Array())	{
		$this->map = $map;
		$this->reduce = $reduce;
		$this->query = $query;		
		
		if (!is_null($outputCollection))
			$this->outputCollection = $outputCollection;
			
		$this->keepTemporaryCollection = (bool)$keepTemporaryCollection;
		
		if (!is_null($finalize))
			$this->finalize = $finalize;
			
		if (!is_int($limit))
			$this->limit = $limit;
				
		$this->sort = $sort;
		$this->scope = $scope;
	}
	
	/**
	 * 
	 * @param MongoDB $mongoDB
	 * @return MongoMapReduceResponse
	 */
	public function invoke(MongoDB $mongoDB, $collection)	{
		$this->collectionNamespace = $collection;
		$response =  $mongoDB->command($this->prepare()->_mapReduce);		
		return new MongoMapReduceResponse($mongoDB, $response);
	}
	
	/**
	 * prepare map reduce parameters to be executed
	 * @return MongoMapReduce
	 */
	private function prepare()	{
		$this->_mapReduce = Array();
		$this->_mapReduce[self::KEYS_MAPREDUCE] = $this->collectionNamespace;
		$this->_mapReduce[self::KEYS_MAP] = $this->map;
		$this->_mapReduce[self::KEYS_REDUCE] = $this->reduce;
		$this->_mapReduce[self::KEYS_KEEP_TEMPORARY_COLLECTION] = $this->keepTemporaryCollection;
		
		if (!empty($this->query))
			$this->_mapReduce[self::KEYS_QUERY] = $this->query;
		
		if (isset($this->finalize))
			$this->_mapReduce[self::KEYS_FINALIZE] = $this->finalize;
		if (isset($this->outputCollection))
			$this->_mapReduce[self::KEYS_OUTPUT_COLLECTION] = $this->outputCollection;
			
		if (!empty($this->sort))
			$this->_mapReduce[self::KEYS_OUTPUT_SORT] = $this->sort;
			
		if (isset($this->limit))
			$this->_mapReduce[self::KEYS_OUTPUT_LIMIT] = $this->limit;

		if (!empty($this->scope))
			$this->_mapReduce[self::KEYS_OUTPUT_SCOPE] = $this->scope;
			
		return $this;
	}
	
	
	///SETTER METHODS///
	
	public function setMap($map)	{
		$this->map = $map;
		return $this;
	}
	
	public function setReduce($reduce)	{
		$this->reduce = $reduce;
		return $this;
	}
	
	public function setQuery(Array $query)	{
		$this->query = $query;
		return $this;
	}
	
	public function setKeepTemporaryCollection($keepTemp = TRUE)	{
		if (is_bool($keepTemp))	
			$this->keepTemporaryCollection = $keepTemp;
		return $this;
	}
	
	public function setOutputCollection($outputCollection)	{
		if ($outputCollection != '')
			$this->outputCollection = $outputCollection;
		return $this;
	}
	
	public function setFinalize($finalize)	{
		if ($finalize != '')
			$this->finalize = $finalize;
		return $this;
	}
	
	public function setLimit($limit)	{
		if (is_int($limit))	
			$this->limit = $limit;
		return $this;
	}
	
	public function setScope(Array $scope)	{		
		$this->scope = $scope;
		return $this;
	}
	
	public function setSort(Array $sort)	{
		$this->sort = $sort;
		return $this;
	}
}