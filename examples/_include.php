<?php
function __autoload($class_name) {
    $dirname = dirname(dirname(__FILE__));
    $path = "{$dirname}/lib/{$class_name}.php";
    require $path;
}
