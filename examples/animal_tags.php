<?php
function __autoload($class_name) {
    require_once "../lib/".$class_name . '.php';
}

$db_name = "test_dbs";
$mongodb = new MongoDB(new Mongo(), $db_name);

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

$map_reduce = new MongoMapReduce($map, $reduce);
$collection_name = "animal_tagsaa";
$response = $map_reduce->invoke($mongodb, $collection_name); 
print_r($response->getRawResponse());
if ($response->valid())	{
	echo "Total Execution Time: {$response->getTotalExecutionTime()} Milli Seconds\n";
	$count_data = $response->getCountsData();
	
	echo "Count Data\n";
	foreach ($count_data as $key=>$value)	{
		echo "{$key}: {$value}\n";
	}
	echo "********************\n";
	foreach ($response->getResultSet() as $tag)	{
		echo "{$tag["_id"]}\n";
		echo "Count: {$tag["value"]["count"]}\n";
		echo "****************\n";
	}
}
