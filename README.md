# Gate

A distributed forensic process system that is designed to scale from a single
machine to thousands of servers. Gate is optimized for multi-tenancy and high
concurrency.

Gate provides a simple, REST-based API for communication.

[![Build Status](https://travis-ci.org/vindeka/gate.png?branch=master)](https://travis-ci.org/vindeka/gate)

## Code Organization

 * bin/: Executable scripts that are the processes run by the deployer
 * etc/: Sample config files
 * gate/: Core code
    * common/: code shared by different modules
    * engine/: engine server
    * hash/: hash server
    * index/: index server
    * process/: process server
 * test/: Unit tests
 * tools/: Used by setuptools

## Data Flow

Gate is a WSGI application and uses eventlets's WSGI server. All communication
between the WSGI application and the servers is done through worker queues
supported by the Kombu framework. A message is placed on the queue and the
corrisponding workers take the request, process, and respond. 

## Servers

Gate has multiple processes each dedicated to complete a specific task. This
allows the deployer the ability to adjust the number of each worker type based
on the work load of their system.

### Engine Server

The engine server performs all request that will have a presistent affect on the
information. Example: when a new file is submitted for processing then the
engine server will handle the request, update the database, and submit the file
for processing.

### Process Server

This server analyzes all of the data through a pipeline. As the data passes
through the pipeline, meta data is built for the file. Once finished processing
the meta data is pased off to the engine server to save to the database. If more
files are found when processing the file, then those files are published on the
queue to be processed with the same pipeline as its parent file.

