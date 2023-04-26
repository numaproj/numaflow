# Sinks
A sink serves as the endpoint for processed data that has been outputted from the platform,
which is then sent to an external system or application. The purpose of a sink is to deliver 
the processed data to its ultimate destination, such as a database, data warehouse, visualization 
tool, or alerting system. It's the opposite of a source, which receives input data into the platform.
To fulfill its role, a sink component may require the transformation or formatting of data prior to 
sending it to the target system. Depending on the target system's needs, this transformation can be 
simple or complex.

Numaflow currently supports:-

* Log sink
* Black Hole Sink
* User defined sinks

A user-defined sink is a custom sink component that a user can define and implement in Numaflow when 
the user needs to output the processed data to a system or using a certain transformation that is not 
supported by the platform's built-in sinks. 

As an example, once we have processed the input messages, 
we can use Elasticsearch as a User defined sink to store the processed data and enable search and analysis
on the data.