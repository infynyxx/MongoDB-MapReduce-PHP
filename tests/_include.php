<?php
require_once 'PHPUnit/Framework.php';

function __autoload($class_name) {
    require_once "lib/".$class_name . '.php';
}

/** **/