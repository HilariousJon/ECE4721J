#!/bin/bash

tail -n +2 | awk -F, '{ print $2 "\t" $3 }'