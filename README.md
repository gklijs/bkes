# Binary Kafka backed Event Store

This is currently a POC status project and should not be used in production. Giving it a try and providing feedback on
GitHub Discussions is very much appreciated. The main goal is to have a fast, low memory event store backed by a Kafka
topic.

## Design principles

The core principle is that Kafka should be the source of truth, so on startup it synchronizes with Kafka. This means it
will read all the messages from the last committed offset, and makes sure the db will contain exactly the binaries
present on the Kafka topic.

For now, it's assumed most one `bkes` instance is available for one topic at a time, to prevent concurrent writes of the
same topic and order. A nicer future solution would route traffic to the 'leader' of a specific partition. For now
things are kept simple. A second instance might be started, as long as the api can't be called, to create a new restore
point.

Another principle is to stay as close as possible to the Kafka Api for it to work. This means it doesn't expect any
specific headers or such, and can be used with an existing Kafka topic. Also, by sticking to binaries being able to read
those bytes in a useful way is not part of the responsibility of `bkes`. For it to accept a new event, the only check
that's done is making sure the order is the same the caller of the api expects.

For the `group.id` property of the consumers the fixed value `bkes` is used. The consumer lag for this id, is the amount
of data not flushed to disk. It's important to remove the committed offsets if the data volume is lost or when you want
to build up the db from start again.