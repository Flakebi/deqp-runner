#!/usr/bin/env bash
# Sort if has two arguments
# Check if sorted otherwise
if [[ $# == 2 ]]; then
	sort -n < "$1" | while read -r line; do
		printf 'TEST: %s\n' "$line"
	done
else
	if sort -n < "$1" | diff "$1" /dev/stdin; then
		while read -r line; do
			printf "Test case '%s'..\n  Pass (Result image matches reference)\n" "$line"
		done < "$1"
		printf 'DONE!\n'
	else
		test=`head -n1 "$1"`
		printf "Test case '%s'..\n  Fail (List is not sorted)\n" "$test"
	fi
fi
