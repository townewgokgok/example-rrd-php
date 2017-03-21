#!/bin/bash

cd $( dirname "$0" )
rm -f data/*.{rrd,png}
php index.php

