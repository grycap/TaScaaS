
#!/bin/bash

nFiles=$1
nNumbers=$2

rm -r genFiles &> /dev/null
mkdir genFiles

for ((j = 0 ; j < $nFiles ; j++))
do
	for ((i = 0 ; i < $nNumbers ; i++))
	do
		echo -e "$((1 + $RANDOM % 10000))$(($RANDOM % 10000))$(($RANDOM % 10000))$(($RANDOM % 10000))$(($RANDOM % 10000))$(($RANDOM % 10000))$(($RANDOM % 10000))" >> genFiles/file_$j
	done
done

