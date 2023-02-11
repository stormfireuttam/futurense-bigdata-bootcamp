#!/bin/bash

mkdir demo
touch demo/file1.txt
touch demo/.file2.txt
echo "Hello Futurense!" > demo/file1.txt
echo "Bye Futurense!!" > demo/.file2.txt
ls -a demo
