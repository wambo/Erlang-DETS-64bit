#!/bin/sh
echo wDETS alá ful-variant startas...
erl \
	-pa $PWD/ebin \
	-s make all

