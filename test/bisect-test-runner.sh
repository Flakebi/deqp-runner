#!/usr/bin/env bash
# if $1 is in $8 return $2/3/4, else $5/6/7
if grep "$1" "$8" > /dev/null; then
	cat $2
	cat $3 > /dev/stderr
	exit $4
else
	cat $5
	cat $6 > /dev/stderr
	exit $7
fi
