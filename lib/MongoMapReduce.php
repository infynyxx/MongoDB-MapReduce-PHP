<?php

class MongoMapReduce
{
	private $_mapReduce;
	
	private $_response;
	
	private $mongoDB;
		
	private $collectionNamespace;
	
	private $map;
	
	private $reduce;
	
	private $outputCollection;
	
	private $keepTemporaryCollection;
	
	private $finalize;
	
	private $query = Array();
	
	const VALUE = 'value';	//value index for MR Result Set
	
	const COUNT = 'count';	//number of objects in a MR Result Set
	
	const KEYS_MAPREDUCE = "mapreduce";
	const KEYS_MAP = "map";
	const KEYS_REDUCE = "reduce";
	const KEYS_KEEP_TEMPORARY_COLLECTION = "keeptemp";
	const KEYS_FINALIZE = "finalize";
	const KEYS_OUTPUT_COLLECTION = "out";
	
	public function __construct($map, $reduce, Array $query = Array(), $outputCollection = NULL, $keepTemporaryCollection = FALSE, $finalize = NULL)	{
		$this->map = $map;
		$this->reduce = $reduce;
		$this->query = $query;
		
		if (!is_null($outputCollection))
			$this->outputCollection = $outputCollection;
			
		$this->keepTemporaryCollection = (bool)$keepTemporaryCollection;
		
		if (!is_null($finalize))
			$this->finalize = $finalize;
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
		
		if (isset($this->finalize))
			$this->_mapReduce[self::KEYS_FINALIZE] = $this->finalize;
		if (isset($this->outputCollection))
			$this->_mapReduce[self::KEYS_OUTPUT_COLLECTION] = $this->outputCollection;
		return $this;
	}
}