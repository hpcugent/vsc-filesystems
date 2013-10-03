# vsc-filesystems

This Python package provides a number of tools to deal with filesystem operations, 
wrapping around the default Python API from os and os.path.

Currently, we support the following filesystems:

- GPFS
- POSIX


## GPFS

The General Parallel FileSystem is a filesystem [1] designed for an HPC environment, created and 
maintained by IBM.

We support requesting filesystem information through the shipped gpfs command suite, which can 
produce machine parsable output in a colon-separated format. The following operations are available.

- listing all filesystems in the GPFS cluster with their atttributes
- listing all filesets in each of the filesystems with their attributes
- adding a fileset
- adding, changing and listing quota information for user, group and fileset
- adding and changing grace periods in case of exceeded quota


## POSIX

Wraps around the common os utilities, adding logging and exception handling.

[1] http://www.ibm.com/systems/software/gpfs/
