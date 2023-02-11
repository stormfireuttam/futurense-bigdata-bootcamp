#!/bin/bash

echo "Enter first number: "
read num1
echo "Enter second number: "
read num2
val1=`expr $num1 + $num2`
val2=`expr $num1 - $num2`
val3=`expr $num1 \* $num2`
val4=`expr $num1 / $num2`
echo "Addition: $val1"
echo "Subtraction: $val2"
echo "Multiplication: $val3"
echo "Division: $val4"

