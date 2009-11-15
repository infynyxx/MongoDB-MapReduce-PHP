<?php
function __autoload($class_name) {
    require_once "../lib/".$class_name . '.php';
}

$db_name = "test_dbs";
$mongodb = new MongoDB(new Mongo(), $db_name);

$map = <<<MAP
	function()	{
		emit(this.p, {data: this});
	}
MAP;

$interval = "day";
$start_date = "Nov 05 2009";
$end_date = "Dec 15 2009";

$reduce = <<<REDUCE
	function(key, values)	{
		var visits = 0, unique_visits = 0, ts, interval = "$interval";
		var start_date = new Date("$start_date");		
		var visit_info = [];
		
		if (interval === "day")	{			
			for (var i = 0; i < values.length; i++)	{
				visits = values[i].data.v;				
				var temp = {visits: visits, ts: values[i].data.ts};
				visit_info.push(temp);				
			}
		}
		else if (interval === "week")	{
			var end_date = new Date("$end_date");
			var days = (end_date.getTime() - start_date.getTime()) / 1000 / 60 / 60 / 24;
			var interval_count = Math.ceil(days/7);			
			for (var j = 0; j < interval_count; j++)	{
				var end_date2 = new Date();
				end_date2.setTime(start_date.getTime() + (1000 * 60 * 60 * 24 *7));				
				for (var i = 0; i < values.length; i++)	{					
					if (values[i].data.ts >= start_date && values[i].data.ts < end_date2)	{
						visits += values[i].data.v;
					}					
				}
				
				if (visits > 0)	{
					var temp = {visits: visits, ts: start_date};
					visit_info.push(temp);
				}
				start_date = end_date2;
				visits = 0;
			}		
		}
		else if (interval === "month")	{
			var month = start_date.getMonth(), j = 0, temp = {};
			for (var i = j; i < values.length; i++)	{					
				var month2 = values[i].data.ts.getMonth();
				if (month2 === month)	{
					visits += values[i].data.v;
					if (i == (values.length -1))	{
						temp = {visits: visits, ts: start_date};
						visit_info.push(temp);
					}
				}
				else	{
					temp = {visits: visits, ts: start_date};
					visit_info.push(temp);
					month = month2;					
					start_date = values[i].data.ts;
					start_date.setDate(1);
					visits = 0;
					j = i--;
				}
				
			}
		}
		return {count: values.length, visit_info: visit_info};
	}
REDUCE;


$query = Array(
	"ts" => Array(
		'$gte' => new MongoDate(strtotime($start_date)),
		'$lt' => new MongoDate(strtotime($end_date))
	)
);

$map_reduce = new MongoMapReduce($map, $reduce, $query);
$collection_name = "usage_daily_partners";
$response = $map_reduce->invoke($mongodb, $collection_name); 

if ($response->valid())	{
	echo "Total Execution Time: {$response->getTotalExecutionTime()} Milli Seconds\n";
	$count_data = $response->getCountsData();
	$result = $response->getResultSet();
	foreach ($result as $r)	{
		echo "ID: {$r["_id"]}\n";
		foreach ($r["value"]["visit_info"] as $visit_info)	{
			$visits_count = $visit_info["visits"];
			//if ($visits_count > 0)	{
				echo "Visits: {$visits_count}****". date("Y-m-d", $visit_info["ts"]->sec) ."}\n";
			//}	
		}
		echo "\n";
		echo "********************\n";
	}
}
