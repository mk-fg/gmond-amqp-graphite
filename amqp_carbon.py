#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

####################

carbon_default_port = 2003

####################

from utils import AttrDict, configure_logging, AMQPLink, node_id

import itertools as it, operator as op, functools as ft
from time import time, sleep
from json import loads
import os, sys, logging


class CarbonClient(object):

	def __init__(self, remote, reconnect_delay=5, max_reconnects=None):
		self.log = logging.getLogger('amqp_carbon.carbon_client')
		self.remote = remote
		if max_reconnects is not None\
			and max_reconnects <= 0: max_reconnects = None
		self.max_reconnects = max_reconnects
		self.reconnect_delay = reconnect_delay
		self.connect()

	def connect(self):
		reconnects = self.max_reconnects
		while True:
			self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			try:
				self.sock.connect(self.remote)
				self.log.debug('Connected to Carbon at {}:{}'.format(*self.remote))
				return
			except socket.error as err:
				if reconnects is not None:
					reconnects -= 1
					if reconnects <= 0: raise
				self.log.info('Failed to connect to {0[0]}:{0[1]}: {1}'.format(self.remote, err))
				if self.reconnect_delay: sleep(max(0, self.reconnect_delay))

	def reconnect(self):
		self.close()
		self.connect()

	def close(self):
		try: self.sock.close()
		except: pass

	def send(self, stat, val, ts):
		reconnects = self.max_reconnects
		msg = '{}\n'.format(' '.join([stat, bytes(val), bytes(ts)]))
		while True:
			try:
				self.sock.sendall(msg)
				return
			except socket.error as err:
				if reconnects is not None:
					reconnects -= 1
					if reconnects <= 0: raise
				log.error('Failed to send data to Carbon server: {}'.format(err))
				self.reconnect()


class AMQPHarvester(AMQPLink):

	def __init__(self, *argz, **kwz):
		self.carbon = kwz.pop('destination')
		self.dry_run = kwz.pop('dry_run', False) # still consumes stuff from queue
		self.dump = kwz.pop('dump', False)
		self.exclusive = kwz.pop('exclusive', False) # force 1 consumer per node per queue

		if not self.dry_run: self.carbon_link = CarbonClient(self.carbon)
		self.queue = 'harvester.{}'.format(':'.join(self.carbon))
		super(AMQPHarvester, self).__init__(*argz, **kwz)

	def schema_init(self):
		super(AMQPHarvester, self).schema_init()
		# TODO: http://www.rabbitmq.com/extensions.html#queue-ttl
		self.ch.queue_declare(queue=self.queue, durable=True)
		self.ch.queue_bind( queue=self.queue,
			exchange=self.exchange.name, routing_key='%' )

	def decode(self, buff, content_type):
		if content_type == 'application/x-gmond-amqp-1': return loads(buff)
		else: raise NotImplementedError('Unknown content type: {}'.format(content_type))

	def process(self, ch, method, head, body):
		metric, ts, val, val_raw = self.decode(body, content_type=head.content_type)
		if not self.dry_run: self.carbon_link.send(metric, val, ts)
		if self.dump: print('{} {} {}'.format(metric, val, ts))
		ch.basic_ack(method.delivery_tag) # should pass exceptions to loop-starter

	def harvest(self):
		consumer_kwz = dict() if not self.exclusive\
			else dict(consumer_tag=self.queue + node_id())
		while True:
			try:
				if not self.link: raise self.PikaError
				tag = self.ch.basic_consume(
					self.process, queue=self.queue, **consumer_kwz )
				try: self.ch.start_consuming() # infinite loop
				finally:
					try: self.ch.basic_cancel(consumer_tag=tag)
					except: pass # so it won't mess up original exception
			except (self.PikaError, socket.error) as err:
				(self.log.error if not log_tracebacks else self.log.exception)\
					('Severed connection to AMQP broker: {}'.format(err))
				self.connect()
			else:
				raise self.PikaError('Consumer loop broke without raising any error')


def main():
	global log_tracebacks # can be updated

	import argparse
	parser = argparse.ArgumentParser(
		description='Collect metrics from amqp and dispatch them to carbon daemon.')
	parser.add_argument('-c', '--config', action='append', default=list(),
		help='Additional configuration files to read. Can be specified'
			' multiple times, values from later ones override values in the former.')
	parser.add_argument('-n', '--dry-run', action='store_true', help='Do not actually send data.')
	parser.add_argument('--dump', action='store_true', help='Dump polled data to stdout.')
	parser.add_argument('--debug', action='store_true', help='Verbose operation mode.')
	optz = parser.parse_args()

	cfg = AttrDict.from_yaml('{}.yaml'.format(
		os.path.splitext(os.path.realpath(__file__))[0] ), if_exists=True)
	for k in optz.config: cfg.update_yaml(k)
	configure_logging( cfg.logging,
		logging.DEBUG if optz.debug else logging.WARNING )
	logging.captureWarnings(cfg.logging.warnings)

	optz.dump = optz.dump or cfg.debug.dump_data
	optz.dry_run = optz.dry_run or cfg.debug.dry_run
	log_tracebacks = cfg.logging.tracebacks

	dst = cfg.net.carbon.host
	if isinstance(dst, types.StringTypes):
		dst = dst.rsplit(':', 1)
		dst = dst[0], int(dst[1]) if len(dst) > 1 else cfg.net.carbon.default_port

	amqp = AMQPHarvester(
		host=cfg.net.amqp.host,
		auth=(cfg.net.amqp.user, cfg.net.amqp.password),
		exchange=cfg.net.amqp.exchange, heartbeat=cfg.net.amqp.heartbeat,
		libc_gethostbyname=not cfg.net.bypass_libc_gethostbyname,
		log=logging.getLogger('amqp_carbon.amqp_link'),
		destination=dst, dry_run=optz.dry_run, dump=optz.dump )

	log = logging.getLogger('amqp_carbon.main_loop')

	log.debug('Waiting for requests')
	try: ch.start_consuming() # infinite loop
	except KeyboardInterrupt: pass
	finally: ch.basic_cancel(consumer_tag=mq_local_id)
