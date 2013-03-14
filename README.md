# Gate

A distributed forensic process system that is designed to scale from a single
machine to thousands of servers. Gate is optimized for multi-tenancy and high
concurrency.

Gate provides a simple, REST-based API provided by gateswift middleware.

## Code Organization

 * bin/: Executable scripts that are the processes run by the deployer
 * etc/: Sample config files
 * gate/: Core code
    * common/: code shared by different modules
    * engine/: engine server
    * hash/: hash server
    * index/: index server
    * process/: process server

## Data Flow

All communication between the gateswift middleware and the processing system is
done through worker queues (AMPQ). A work request is placed on the queue and the
corrisponding workers take the request, process, and respond. 

## Servers

Gate has multiple processes each dedicated to complete a specific task. This
allows the deployer the ability to adjust the number of each worker type based
on the work load of their system.

### Engine Server

The engine server performs all request that will have a presistent affect on the
information. Example: when a new file is idenitified by the process server, then
the engine server will place this information in the Swift storage.

All new requests are also handled by the engine server. When a new request is
placed to process or verify data, then the engine server places the actual
request for the process to begin with the process servers.

### Hash Server

Only action this server performs is hashing data to provide hash values for that
data.

### Index Server

Only action this server performs is indexing data. When a file is identified,
then its content is indexed by this server.

### Process Server

This server analyzes all of the data through a pipeline. As the data passes
through the pipeline, information is built for the file. If more files are found
when processing the file, then those files are published on the queue to be
processed with the same pipeline as its parent file.

