#!/bin/bash
eval=`expr 2 + 2`
echo "Total Value: $eval"

a=100
b=50
val1=`expr $a + $b`
echo "A + B : $val1"
val2=`expr $a - $b`
echo "A - B : $val2"
val3=`expr $a \* $b`
echo "A * B : $val3"
val4=`expr $a / $b`
echo "A / B : $val4"
