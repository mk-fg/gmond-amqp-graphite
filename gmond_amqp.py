#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

####################

graphite_min_cycle = 60
log_tracebacks = True

####################

from socket import gethostbyname, gethostname # gevent works around libc
from gevent import monkey
monkey.patch_all()

from gevent.select import select
from gevent.queue import Queue, Full, Empty
from gevent.event import Event, AsyncResult
from gevent.pool import Group
from gevent.coros import Semaphore
from gevent import greenlet, socket, Timeout, GreenletExit
import gevent

from lxml import etree

from pika.adapters import BlockingConnection
from pika.credentials import PlainCredentials
from pika import BasicProperties, ConnectionParameters
import pika.log

try:
	import pika.adapters.blocking_connection
	pika.adapters.blocking_connection.log
except ImportError: pass
except AttributeError:
	# Fixup for 0.9.5 bug - "log" global is undeclared there
	pika.adapters.blocking_connection.log = pika.log

import itertools as it, operator as op, functools as ft
from pprint import pprint
from contextlib import closing
from collections import Iterable
from time import time, sleep
from io import BytesIO
import os, sys, logging, types, re


class DataPollError(Exception): pass

def gmond_poll( sources,
		timeout=graphite_min_cycle, to_escalate=None, to_break=None,
		src_escalate=[1, 1, 2.0], default_port=8649, libc_gethostbyname=True ):
	'''XML with values is fetched from possibly-multiple sources,
			first full dump received is returned.
		sources: iterable of sources to query - either hostname/ip or tuple of (hostname/ip, port)
		src_escalate:
			# number of sources to query simultaneously in the beginning and add after each to_escalate
			int - how many sources to query after each to_escalate passes,
			float (0-1.0) - percentage of sources,
			or iterable of ints/floats - value to use for each step, last one being used for the rest
		to_escalate: # timeout before starting querying additional sources
			int/float or iterable of these ([1,2,3] would mean "wait 1s, then 2s, then 3s")
		to_break: int/float # timeout to stop waiting for data for one source (break connection)
		timeout: int/float # global timeout
			(not counting libc.gethostbyname for all sources, if used),
			also used to calculate sensible values for to_*, if none specified'''
	log = logging.getLogger('gmond_amqp.poller')

	# Otherwise gevent does it's own (although parallel)
	#  gethostbyname, ignoring libc (ldap, nis, /etc/hosts), which is wrong
	# Obvious downside, is that it's serial - i.e. all hosts will be resolved here and now,
	#  before any actual xml fetching takes place, can be delayed but won't suck any less
	libc_gethostbyname = gethostbyname if libc_gethostbyname else lambda x: x
	sources = list(
		(libc_gethostbyname(src[0]), int(src[1]) if len(src)>1 else default_port)
		for src in it.imap(op.methodcaller('rsplit', ':', 1), sources) )

	# First calculate number of escalation tiers, then pick proper intervals
	src_escalate = list(reversed( src_escalate
		if isinstance(src_escalate, Iterable) else [src_escalate] ))
	src_slice, src_count = src_escalate.pop(), len(sources)
	src_tiers = list()
	while sources:
		src_tier, sources = sources[:src_slice], sources[src_slice:]
		src_tiers.append(src_tier)
		if src_escalate: src_slice = src_escalate.pop()
		if isinstance(src_slice, float): src_slice = int(src_count / src_slice)

	if to_escalate is None:
		to_escalate = [ 1, # 1s should be enough for everyone!
			((timeout - 1) / 2.0) / ((len(src_tiers) - 1) or 1) ] # so they'll fit in half-timeout
	if not isinstance(to_escalate, Iterable): to_escalate = [to_escalate]
	if to_break is None: to_break = timeout
	src_tiers = zip(it.chain(to_escalate, it.repeat(to_escalate[-1])), src_tiers)
	log.debug('Escalation tiers: {}'.format(src_tiers))

	def fetch_from_src(source):
		try:
			with Timeout(to_break),\
					closing(socket.socket(
						socket.AF_INET, socket.SOCK_STREAM )) as sock:
				log.debug('Fetching from source: {}'.format(source))
				sock.connect(source)
				buff = bytes()
				while True:
					chunk = sock.recv(1*2**20)
					if not chunk: return buff
					buff += chunk
		except (Timeout, socket.error) as err:
			log.debug('Connection to source {} failed ({err})'.format(source, err=err))
			return DataPollError # indicates failure

	src_tiers = list(reversed(src_tiers))
	queries, result, sentinel = Group(), Queue(), None
	try:
		with Timeout(timeout):
			while src_tiers:
				to, src_tier = src_tiers.pop()
				for src in src_tier:
					src = queries.spawn(fetch_from_src, src)
					src.link(result.put)
					src.link_exception()
				if sentinel is None or sentinel.ready():
					sentinel = gevent.spawn(queries.join)
					sentinel.link(result.put) # to break/escalate if they all died
				try:
					with Timeout(to if src_tiers else None):
						while True:
							res = result.get(block=True).get(block=True, timeout=0)
							if res is None: raise Timeout
							elif res is not DataPollError: return res
				except Timeout: pass
				if src_tiers: log.debug('Escalating to the next tier: {}'.format(src_tiers[-1]))
				else: raise Timeout
	except Timeout: raise DataPollError('No sources could be reached in time')
	finally: queries.kill(block=True)


def gmond_xml_process( xml,
		validate=True, validate_strict=False,
		log=logging.getLogger('gmond_amqp.xml_parser') ):
	'Process gmond XML data into tuples of (host_data, metrics).'
	# Don't see much point in iterative parsing here,
	#  since whole XML is cached into RAM anyway.
	# Alternative is to pull it from socket through parser or cache into a file,
	#  which may lead to parsing same XML multiple times if first link is slow.
	xml = etree.parse(BytesIO(xml.replace('\n', '')))

	# Validation is kinda optional, but why not?
	if validate or validate_strict:
		dtd = xml.docinfo.internalDTD
		if not dtd.validate(xml):
			err = 'XML validation failed, errors:\n{}'.format(
					'\n'.join(it.imap(bytes, dtd.error_log.filter_from_errors())) )
			if validate_strict: raise AssertionError(err)
			else: log.warn(err)

	for cluster in xml.iter('CLUSTER'):
		yield cluster.attrib, list(
			(host.attrib, map(op.attrgetter('attrib'), host.iter('METRIC')))
			for host in cluster.iter('HOST') )


class DataMangler(object):

	class IgnoreValue(Exception): pass

	def __init__(self, name_template):
		self.log = logging.getLogger('gmond_amqp.data_mangler')
		self.name_template = name_template

		self._cache_dict = dict()
		self._cache_check_timeout = 12 * 3600
		self._cache_check_count = 4

	def _cache(self, cache_id, val_id, val=None, ts=None):
		ts_now = ts or time()
		if cache_id not in self._cache_dict: # init new cache type
			self._cache_dict[cache_id] = dict(), ts_now
		cache, ts = self._cache_dict[cache_id]
		if ts_now > ts: # cleanup
			cleanup_list = list( k for k, (v, ts_chk) in
				cache.viewitems() if (ts_now - self._cache_check_timeout) > ts_chk )
			self.log.debug(( 'Cache {!r} cleanup:'
				' {} buckets' ).format(cache_id, len(cleanup_list)))
			for k in cleanup_list: del cache[k]
			self._cache_dict[cache_id] = cache, ts_now\
				+ self._cache_check_timeout / self._cache_check_count
		if val is None: return cache[val_id][0]
		else:
			cache[val_id] = val, ts_now
			return val

	def derive(self, name, mtype, value, ts=None):
		ts_now = time()
		cache = ft.partial(self._cache, 'counters', ts=ts_now)
		ts = ts or ts_now
		if mtype == 'counter':
			try: v0, ts0, _ = cache(name)
			except KeyError:
				self.log.debug('Initializing bucket for new counter: {}'.format(name))
				cache(name, (value, ts))
				raise self.IgnoreValue(name)
			value = float(value - v0) / (ts - ts0)
			cache(name, (value, ts))
			if value < 0:
				self.log.debug( 'Detected counter overflow'
					' (negative delta): {}, {} -> {}'.format(name, v0, value) )
				raise self.IgnoreValue(name)
		elif mtype == 'gauge': value = value
		elif mtype == 'timestamp':
			try: ts = cache(name)
			except KeyError: ts = None
			if ts == value:
				log.debug( 'Ignoring duplicate'
					' timestamp value for {}: {}'.format(name, value) )
				raise self.IgnoreValue(name)
			value, ts = 1, cache(name, value)
		else: raise TypeError('Unknown type: {}'.format(mtype))
		return value, ts

	def process_value( self, metric,
			_vtypes=dict(
				int=re.compile('^int\d+$'),
				uint=re.compile('^uint\d+|timestamp$'),
				float={'float', 'double'} ) ):
		val, vtype, vslope = op.itemgetter('VAL', 'TYPE', 'SLOPE')(metric)
		if vtype == 'string': val = bytes(val)
		elif _vtypes['int'].search(vtype): val = int(val)
		elif _vtypes['uint'].search(vtype):
			val = int(val)
			assert val > 0
		elif vtype in _vtypes['float']: val = float(val)
		if vtype == 'timestamp': mtype = 'timestamp'
		elif vslope == 'positive': mtype = 'counter'
		elif vslope in 'negative': mtype = 'derive'
		elif vslope in {'zero', 'both'}: mtype = 'gauge'
		else:
			raise TypeError( 'Unable to handle'
				' value slope/type: {}/{}'.format(vslope, vtype) )
		return val, mtype

	def process_metric(self, name, host, metric, ts=None):
		ts = ts or time()
		val_raw, mtype = self.process_value(metric)
		val, ts = self.derive(name, mtype, val_raw, ts)
		return ts, val, val_raw

	def process_name( self,
			cluster_name, host_name, metric_name, ts=None ):
		cache_key = cluster_name, host_name
		cache = ft.partial(self._cache, 'names', ts=ts or time())
		try: parts = cache(cache_key)
		except KeyError:
			parts = dict(
				it.chain.from_iterable(
					( ('{}_first_{}'.format(label, i), '.'.join(first)),
						('{}_first_{}_rev'.format(label, i), '.'.join(reversed(first))),
						('{}_last_{}'.format(label, i), '.'.join(last)),
						('{}_last_{}_rev'.format(label, i), '.'.join(reversed(last))) )
					for i, label, first, last in (
						(i, label, name.split('.')[:-i], name.split('.')[i:])
						for i, (label, name) in it.product( xrange(5),
							[('cluster', cluster_name), ('host', host_name)] ) ) ))
			for label in ['cluster', 'host']:
				parts['{}_short'.format(label)] = parts['{}_first_1'.format(label)]
			cache(cache_key, parts)
		return self.name_template.format(**dict(it.chain(
			parts.viewitems(), [('metric', metric_name)] )))

	def process_cluster( self, cluster, hosts,
			ts_stale_limit=graphite_min_cycle, ts=None ):
		ts_now = ts or time()
		if abs(int(cluster['LOCALTIME']) - ts_now) > ts_stale_limit:
			log.warn(( 'Localtime for cluster {0[NAME]} ({0}) is way off'
				' the local timestamp (limit: {1})' ).format(cluster, ts_stale_limit))
		for host, metrics in hosts:
			tn, ts_host = it.imap(int, op.itemgetter('TN', 'REPORTED')(host))
			ts_host += tn
			if tn > ts_stale_limit:
				log.error(( 'Data for host {0[NAME]} ({0}) is too'
					' stale (limit: {1}), skipping metrics for host' ).format(host, ts_stale_limit))
			elif abs(ts_now - ts_host) > ts_stale_limit:
				log.error(( '"Reported" timestamp for host {0[NAME]}'
					' ({0}) is way off the local timestamp (limit: {1}),'
					' skipping metrics for host' ).format(host, ts_stale_limit))
			else:
				process_name = ft.partial( self.process_name,
					*it.imap(op.itemgetter('NAME'), [cluster, host]) )
				for metric in metrics:
					name = process_name(metric['NAME'])
					try: yield (name,) + self.process_metric(name, host, metric, ts=ts_host)
					except self.IgnoreValue: pass


class AMQPLink(object):

	class PikaError(Exception): pass

	link = None

	def __init__( self, host, auth, exchange,
			heartbeat=True, reconnect_delays=5, libc_gethostbyname=True ):
		self.log = logging.getLogger('gmond_amqp.amqp_link')
		self.host, self.auth, self.exchange, self.heartbeat,\
			self.libc_gethostbyname = host, auth, exchange, heartbeat, libc_gethostbyname
		if isinstance(reconnect_delays, (int, float)): reconnect_delays = [reconnect_delays]
		self.reconnect_delays, self.reconnect_info = reconnect_delays, None
		self.connect() # mainly to notify if it fails at start

	def _error_callback(self, msg, *argz, **kwz):
		raise kwz.pop('err', self.PikaError)(msg)

	def connect(self):
		host = self.host
		if self.libc_gethostbyname: host = gethostbyname(self.host)
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
				exchange = self.exchange.copy()
				self.ch.exchange_declare(exchange=exchange.pop('name'), **exchange)
				self.ch.tx_select()

			except (self.PikaError, socket.error) as err:
				(self.log.error if not log_tracebacks else self.log.exception)\
					('Connection to AMQP broker has failed: {}'.format(err))
				delay = self.reconnect_info and self.reconnect_info[0] # first delay is 0
				if delay:
					self.log.debug('Will retry connection in {}s'.format(delay))
					sleep(delay)
				self.reconnect_info = self.reconnect_info or self.reconnect_delays
				if len(self.reconnect_info) > 1: self.reconnect_info = self.reconnect_info[1:]

			else:
				self.reconnect_info = None
				break

	def publish(self, data):
		while True:
			try:
				if not self.link: raise self.PikaError
				for metric, ts, val, val_raw in data:
					self.ch.basic_publish(
						exchange=self.exchange.name,
						routing_key=metric, body='{} {} {}'.format(metric, ts, val),
						properties=BasicProperties(content_type='application/carbon', delivery_mode=2) )
				self.ch.tx_commit()
				sleep(1)
			except (self.PikaError, socket.error) as err:
				(self.log.error if not log_tracebacks else self.log.exception)\
					('Severed connection to AMQP broker: {}'.format(err))
				self.connect()
			else: break


def main():
	global graphite_min_cycle, log_tracebacks # can be updated

	import argparse
	parser = argparse.ArgumentParser(
		description='Collect various metrics from gmond and dispatch'
			' them graphite-style at regular intervals to amqp (so they can be routed to carbon).')
	parser.add_argument('-c', '--config', action='append', default=list(),
		help='Additional configuration files to read. Can be specified'
			' multiple times, values from later ones override values in the former.')
	parser.add_argument('-n', '--dry-run', action='store_true', help='Do not actually send data.')
	parser.add_argument('--dump', action='store_true', help='Dump polled data to stdout.')
	parser.add_argument('--debug', action='store_true', help='Verbose operation mode.')
	optz = parser.parse_args()

	from utils import AttrDict, configure_logging

	cfg = AttrDict.from_yaml('{}.yaml'.format(
		os.path.splitext(os.path.realpath(__file__))[0] ), if_exists=True)
	for k in optz.config: cfg.update_yaml(k)
	configure_logging( cfg.logging,
		logging.DEBUG if optz.debug else logging.WARNING )
	logging.captureWarnings(cfg.logging.warnings)

	optz.dump = optz.dump or cfg.debug.dump_data
	optz.dry_run = optz.dry_run or cfg.debug.dry_run
	graphite_min_cycle, log_tracebacks = cfg.metrics.interval, cfg.logging.tracebacks

	log = logging.getLogger('gmond_amqp.main_loop')
	mangler = DataMangler(name_template=cfg.metrics.name)
	amqp = AMQPLink( host=cfg.net.amqp.host,
		auth=(cfg.net.amqp.user, cfg.net.amqp.password),
		exchange=cfg.net.amqp.exchange, heartbeat=cfg.net.amqp.heartbeat,
		libc_gethostbyname=not cfg.net.bypass_libc_gethostbyname )

	ts, data = time(), list()
	self_profiling = cfg.metrics.self_profiling and '{}.gmond_amqp'.format(
		socket.gethostname() if cfg.net.bypass_libc_gethostbyname else gethostname() )
	while True:
		ts_now = time()

		xml = gmond_poll( cfg.net.gmond.hosts,
			libc_gethostbyname=not cfg.net.bypass_libc_gethostbyname,
			default_port=cfg.net.gmond.default_port )
		if self_profiling:
			ts_new, ts_prof = time(), ts_now
			val, ts_prof = ts_new - ts_prof, ts_new
			data.append(('{}.poll'.format(self_profiling), ts_now, val, val))

		xml = gmond_xml_process( xml,
			validate=cfg.net.gmond.validate_xml,
			validate_strict=cfg.net.gmond.validate_strict )
		if self_profiling:
			ts_new = time()
			val, ts_prof = ts_new - ts_prof, ts_new
			data.append(('{}.process'.format(self_profiling), ts_now, val, val))

		data.extend(it.chain.from_iterable(it.starmap(
			ft.partial(mangler.process_cluster, ts=ts_now), xml )))
		log.debug('Publishing {} datapoints'.format(len(data)))
		if optz.dump: pprint(data)
		if not optz.dry_run: amqp.publish(data)
		if self_profiling:
			ts_new = time()
			val, ts_prof = ts_new - ts_prof, ts_new
			data = [('{}.publish'.format(self_profiling), ts_now, val, val)]

		while ts <= ts_now: ts += cfg.metrics.interval
		ts_sleep = max(0, ts - time())
		log.debug('Sleep: {}s'.format(ts_sleep))
		sleep(ts_sleep)

if __name__ == '__main__': main()
