# Breadbox: A place to store your data before it goes stale

It is a common pattern to cache and reuse connections to databases and other
microservices. Often, this is handled at the client level; for exmaple, the
pgxpool package manages a number of concurrent connections to a postgres
database. However, there are some places where the client library does not
handle this internally. Managing this adds a lot of bookkeeping. How many
connections do I currently have open? How many should I have? When should I
clean them up?

The goal of breadbox is to implement a simple in-memory cache for your structs
that takes care of most of this bookkeeping for you. It's not the fastest cache nor
the msot complex, but it is designed for flexibility 

## License

This project is released under the terms of the MIT license.
