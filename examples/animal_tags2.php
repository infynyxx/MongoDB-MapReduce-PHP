<?php

require_once "_include.php";

$mongodb = mongodb_fixture();

class AnimalTag extends XMongoCollection	{
	
	const COLLECTION_NAME = "animal_tags";
	
	public function __construct(MongoDB $mongoDB)	{
		$this->collectionName = self::COLLECTION_NAME;
		parent::__construct($mongoDB, $this->collectionName); 
	}
}

$animal_tags = new AnimalTag($mongodb);

$map = <<<MAP
	function()	{
		this.tags.forEach(
			function(x)	{
				emit(x, 1);
			}
		);
	}
MAP;

$reduce = <<<REDUCE
	function(key, values)	{
		return {count: values.length };
	}
REDUCE;

$response = $animal_tags->mapReduce(new MongoMapReduce($map, $reduce));
if ($response->valid())	{
	foreach ($response->getResultSet() as $tag)	{
		echo "{$tag["_id"]}\n";
		echo "Count: {$tag["value"]["count"]}\n";
		echo "****************\n";
	}
}
