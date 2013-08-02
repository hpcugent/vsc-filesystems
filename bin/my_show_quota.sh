#!/bin/bash

# remove all loaded modules
module  purge

# load python 2.6+
module load Python/2.7.5-ictce-4.1.13

/usr/bin/show_quota $@
