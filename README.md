gmond-amqp-graphite: daemon to pull xml data from a cluster of gmond nodes and push them to graphite via amqp
--------------------

Toolkit, consisting of gmond_amqp.py daemon to poll metrics from
[ganglia's](http://ganglia.info/) gmond nodes into amqp and amqp_carbon.py to
read them from amqp queue to [graphite's](http://graphite.readthedocs.org/)
carbon daemons.

Why pull data through amqp broker?

* To add multiple consumers for the same values. Graphite to draw graphs, nagios
  or shinken to issue alerts if values exceed some thresholds.
* Reliable delivery. You can plug-in/disable and restart consumers without any
  loss of data, which gathers safely in broker queues.
* Ease of use. That's what I hate [zmq](http://zeromq.org/) for - try to
  implement reliable delivery, durable queues, with a proper fanout and make it
  configurable on-the-fly. Make it not loose messages on any disconnect and
  reconnect scenarios, throw in some atomicity (transactions), so you can
  read/send batches of values. Some 10-50 lines with amqp and insane amount of
  minefield code otherwise.

Differs from passive collectors like
[graphlia](https://github.com/drawks/graphlia) in that it actually *polls* the
data from redundant gmond nodes at the regular intervals (as graphite needs it),
not just listens for it.

gmond_amqp.py also does the job of rewriting gmond metric names, so they fit
neatly into graphite namespaces.

Check out yaml files (actual configuration) for more info.

Wrote the thing before realizing that gmond can't handle dynamic number of
values, like plugin that collects metrics from
[systemd](http://www.freedesktop.org/wiki/Software/systemd) services' cgroups
without knowing how many will be started in advance, so abandoned the thing when
got to migrating data collectors to the actual gmond plugins. Pity, gmond looked
like a nice and solid thing otherwise.


Requirements:
--------------------

* [python 2.7](http://python.org)
* [PyYAML](http://pyyaml.org)
* [pika](http://pika.github.com)
* [gevent](http://www.gevent.org/) (gmond_amqp only)
* [lxml](http://lxml.de) (gmond_amqp only)
