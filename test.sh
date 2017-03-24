#!/bin/bash

cd $( dirname "$0" )
rm -f data/*.{rrd,png}
time php index.php

