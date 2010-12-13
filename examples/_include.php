<?php

define('PORT', 7000);
define('HOST', 'localhost');
define('DB_NAME', 'test_db');
define('COLLECTION_NAME', 'test_collection');

function __autoload($class_name) {
    $dirname = dirname(dirname(__FILE__));
    $path = "{$dirname}/lib/{$class_name}.php";
    require $path;
}
spl_autoload_register("__autoload");

function mongodb_fixture() {
    $db_name = DB_NAME;
    $host_path = "mongodb://" . HOST . ":" . PORT;
    $mongodb = new MongoDB(new Mongo($host_path), $db_name);
    $collection_name = COLLECTION_NAME;
    $collection = new MongoCollection($mongodb, $collection_name);
    $collection->remove(array());   //remove all documents of that collection

    $fixture = array(
        array('_id' => 1, 'tags' => array('dog', 'cat')),
        array('_id' => 2, 'tags' => array('cat')),
        array('_id' => 3, 'tags' => array('mouse', 'cat', 'dog')),
        array('_id' => 4, 'tags' => array())
    );
    $collection->batchInsert($fixture, array('safe' => true));
    return $mongodb;
}
