SGFS
====

Science Gateway File System

The SGFS aims to access Grid files managed by the LFC file catalog service. This service has been thought to help Science Gateways to manage Grid files from both distributed jobs and Science Gateway' applications through a set of REST queries.
By default operates on LFC files catalogued as:
/grid/<vo name/<sgfs_root>/<sg application name>/<sg user name (display name)>/

... user files

by default the root SGFS directory is 'sgfs'

More information are available from the proof of concept server at: http://rasgrid01.consorzio-cometa.it/sgfs.html