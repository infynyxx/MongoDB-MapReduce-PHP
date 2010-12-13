<?php

require_once "_include.php";

$mongodb = mongodb_fixture();

$map = <<<MAP
	function()	{
		this.tags.forEach(function(x)   {
            emit(x, {count: 1});
        });
	}
MAP;

$reduce = <<<REDUCE
	function(k, v)  {
        var total = 0;
        for (var i = 0; i < v.length; i++)  {
            total += v[i].count;
        }
        return {count: total};
    }
REDUCE;

$map_reduce = new MongoMapReduce($map, $reduce);
$collection_name = "animal_tags";
$response = $map_reduce->invoke($mongodb, $collection_name); 
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
