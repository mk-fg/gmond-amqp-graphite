from pika.adapters import BlockingConnection
from pika.credentials import PlainCredentials
from pika import BasicProperties, ConnectionParameters
import pika.log

import itertools as it, operator as op, functools as ft
from collections import Mapping
from time import time, sleep
import os, sys, socket


class AttrDict(dict):
	def __init__(self, *argz, **kwz):
		for k,v in dict(*argz, **kwz).iteritems(): self[k] = v

	def __setitem__(self, k, v):
		super(AttrDict, self).__setitem__( k,
			AttrDict(v) if isinstance(v, Mapping) else v )
	def __getattr__(self, k):
		if not k.startswith('__'): return self[k]
		else: raise AttributeError # necessary for stuff like __deepcopy__ or __hash__
	def __setattr__(self, k, v): self[k] = v

	@classmethod
	def from_yaml(cls, path, if_exists=False):
		import os, yaml
		if if_exists and not os.path.exists(path): return cls()
		return cls(yaml.load(open(path)))

	def flatten(self, path=tuple()):
		dst = list()
		for k,v in self.iteritems():
			k = path + (k,)
			if isinstance(v, Mapping):
				for v in v.flatten(k): dst.append(v)
			else: dst.append((k, v))
		return dst

	def update_flat(self, val):
		if isinstance(val, AttrDict): val = val.flatten()
		for k,v in val:
			dst = self
			for slug in k[:-1]:
				if dst.get(slug) is None:
					dst[slug] = AttrDict()
				dst = dst[slug]
			if v is not None or not isinstance(
				dst.get(k[-1]), Mapping ): dst[k[-1]] = v

	def update_yaml(self, path):
		self.update_flat(self.from_yaml(path))


def configure_logging(cfg, custom_level=None):
	import logging, logging.config
	if custom_level is None: custom_level = logging.WARNING
	for entity in it.chain.from_iterable(it.imap(
			op.methodcaller('viewvalues'),
			[cfg] + list(cfg.get(k, dict()) for k in ['handlers', 'loggers']) )):
		if isinstance(entity, Mapping)\
			and entity.get('level') == 'custom': entity['level'] = custom_level
	logging.config.dictConfig(cfg)


def node_id():
	return '--'.join(
		it.imap(op.itemgetter(0), it.groupby(it.imap(
			lambda val: open(val).read().strip(),
			it.ifilter(os.path.exists, [ '/etc/machine-id',
				'/var/lib/dbus/machine-id', '/proc/sys/kernel/random/boot_id' ]) ))) )


class AMQPLink(object):

	class PikaError(Exception): pass

	link = None
	tx = True

	def __init__( self, host, auth, exchange,
			heartbeat=False, reconnect_delays=5,
			libc_gethostbyname=None, log=None ):
		self.log = log or logging.getLogger('amqp')
		if heartbeat: raise NotImplementedError
		self.host, self.auth, self.exchange, self.heartbeat,\
			self.libc_gethostbyname = host, auth, exchange, heartbeat, libc_gethostbyname
		if isinstance(reconnect_delays, (int, float)): reconnect_delays = [reconnect_delays]
		self.reconnect_delays, self.reconnect_info = reconnect_delays, None
		self.connect() # mainly to notify if it fails at start

	def schema_init(self):
		exchange = self.exchange.copy()
		self.ch.exchange_declare(exchange=exchange.pop('name'), **exchange)

	def _error_callback(self, msg, *argz, **kwz):
		raise kwz.pop('err', self.PikaError)(msg)

	def connect(self):
		host = self.host
		if self.libc_gethostbyname: host = self.libc_gethostbyname(self.host)
		while True:
			if self.link and self.link.is_open:
				try: self.link.close()
				except: pass

			try:
				self.log.debug('Connecting to AMQP broker ({})'.format(host))
				self.link = BlockingConnection(ConnectionParameters( host,
					heartbeat=self.heartbeat, credentials=PlainCredentials(*self.auth) ))

				# Even with BlockingConnection adapter,
				#  pika doesn't raise errors, unless you set callbacks to do that
				self.link.set_backpressure_multiplier(2)
				self.link.add_backpressure_callback(
					ft.partial(self._error_callback, 'timeout') )
				self.link.add_on_close_callback(
					ft.partial(self._error_callback, 'closed/error') )

				self.ch = self.link.channel()
				self.schema_init()
				if self.tx: self.ch.tx_select() # forces flush

			except (self.PikaError, socket.error) as err:
				self.log.exception('Connection to AMQP broker has failed: {}'.format(err))
				delay = self.reconnect_info and self.reconnect_info[0] # first delay is 0
				if delay:
					self.log.debug('Will retry connection in {}s'.format(delay))
					sleep(delay)
				self.reconnect_info = self.reconnect_info or self.reconnect_delays
				if len(self.reconnect_info) > 1: self.reconnect_info = self.reconnect_info[1:]

			else:
				self.reconnect_info = None
				break
