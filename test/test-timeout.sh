#!/usr/bin/env bash
cat $1
cat $2 > /dev/stderr
sleep 60
exit $3
