#!/usr/bin/env bash
cat $1
cat $2 > /dev/stderr
exit $3
