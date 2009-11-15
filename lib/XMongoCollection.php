<?php
abstract class XMongoCollection extends MongoCollection	{
	protected $collectionName;
	protected $mongodb;
	
	public function __construct(MongoDB $mongoDB, $collection_name)	{
		$this->mongodb = $mongoDB;
		parent::__construct($this->mongodb, $collection_name);
	}
	
	/**
	 * invoke MapReduce Operation
	 * @param MongoMapReduce $mapReduce
	 * @return MongoMapReduceResponse
	 */
	public function mapReduce(MongoMapReduce $mapReduce)	{
		return $mapReduce->invoke($this->mongodb, $this->collectionName);
	}
}