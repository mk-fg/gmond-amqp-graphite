#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

####################

graphite_min_cycle = 60
gmond_default_port = 8649

####################

from socket import gethostbyname # gevent works around libc
from gevent import monkey
monkey.patch_all()

from gevent.select import select
from gevent.queue import Queue, Full, Empty
from gevent.event import Event, AsyncResult
from gevent.pool import Group
from gevent.coros import Semaphore
from gevent import greenlet, socket, Timeout, GreenletExit
import gevent

import itertools as it, operator as op, functools as ft
from contextlib import closing
from collections import Iterable
from time import time, sleep
from lxml import etree
from io import BytesIO
import os, sys, logging, types


class DataPollError(Exception): pass

def gmond_poll( sources,
		timeout=graphite_min_cycle, to_escalate=None, to_break=None,
		src_escalate=[1, 1, 2.0], libc_gethostbyname=True,
		log=logging.getLogger('gmond_amqp.poller') ):
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

	# Otherwise gevent does it's own (although parallel)
	#  gethostbyname, ignoring libc (ldap, nis, /etc/hosts), which is wrong
	# Obvious downside, is that it's serial - i.e. all hosts will be resolved here and now,
	#  before any actual xml fetching takes place, can be delayed but won't suck any less
	libc_gethostbyname = gethostbyname if libc_gethostbyname else lambda x: x
	sources = list(
		(libc_gethostbyname(src[0]), int(src[1]) if len(src)>1 else gmond_default_port)
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
	# help(etree)
	xml = etree.parse(BytesIO(xml.replace('\n', '')))

	# Validation is kinda optional, but why not?
	if validate or validate_strict:
		dtd = xml.docinfo.internalDTD
		if not dtd.validate(xml):
			err = 'XML validation failed, errors:\n{}'.format(
					'\n'.join(it.imap(bytes, dtd.error_log.filter_from_errors())) )
			if validate_strict: raise AssertionError(err)
			else: log.warn(err)

	for host in root.iter('HOST'):
		yield (host.attrib, map(op.attrgetter('attrib'), host.iter('METRIC')))


class DataMangler(object):
	log = logging.getLogger('gmond_amqp.data_mangler')

	def __init__(self):
		self._counter_cache = dict()
		self._counter_cache_check_ts = 0
		self._counter_cache_check_timeout = 12 * 3600
		self._counter_cache_check_count = 4

	def _counter_cache_cleanup(self, ts, to):
		cleanup_list = list( k for k,(v,ts_chk) in
			self._counter_cache.viewitems() if (ts - to) > ts_chk )
		self.log.debug('Counter cache cleanup: {} buckets'.format(len(cleanup_list)))
		for k in cleanup_list: del self._counter_cache[k]

	def derive(self, name, mtype, value, ts=None):
		ts = ts or time()
		if ts > self._counter_cache_check_ts:
			self._counter_cache_cleanup( ts,
				self._counter_cache_check_timeout )
			self._counter_cache_check_ts = ts\
				+ self._counter_cache_check_timeout\
				/ self._counter_cache_check_count
		if mtype == 'counter':
			if name not in self._counter_cache:
				self.log.debug('Initializing bucket for new counter: {}'.format(name))
				self._counter_cache[name] = value, ts
				return None
			v0, ts0 = self._counter_cache[name]
			value = float(value - v0) / (ts - ts0)
			self._counter_cache[name] = value, ts
			if value < 0:
				# TODO: handle overflows properly, w/ limits
				self.log.debug( 'Detected counter overflow'
					' (negative delta): {}, {} -> {}'.format(name, v0, value) )
				return None
		elif mtype == 'gauge': value = value
		else: raise TypeError('Unknown type: {}'.format(mtype))
		return name, value, ts

	def process(self, host, metric, ts=None):
		# 1. Produce metric name from host and metric data
		# 2. Produce derivative metric value, return along with raw one
		raise NotImplementedError

	def process_host(self, host, metrics, ts=None):
		for metric in metrics: yield self.process(host, metric, ts=None)



def main():
	import argparse
	parser = argparse.ArgumentParser(
		description='Collect various metrics from gmond and dispatch'
			' them graphite-style at regular intervals to amqp (so they can be routed to carbon).')
	parser.add_argument('sources', nargs='+',
		help=( 'Hostnames/ips (optional ":port" - defaults to {}, ipv4 only)'
			' of gmond nodes to poll.' ).format(gmond_default_port))
	parser.add_argument('-n', '--dry-run', action='store_true', help='Do not actually send data.')
	parser.add_argument('--bypass-libc-gethostbyname',
		action='store_false', default=True,
		help='Use gevent-provided parallel gethostbyname(),'
			' which only queries DNS servers, without /etc/nsswitch.conf, /etc/hosts'
			' and other (g)libc stuff.')
	parser.add_argument('--debug', action='store_true', help='Verbose operation mode.')
	optz = parser.parse_args()

	logging.basicConfig(
		level=logging.WARNING if not optz.debug else logging.DEBUG,
		format='%(levelname)s :: %(name)s :: %(message)s' )

	log=logging.getLogger('gmond_amqp.main_loop')
	mangler = DataMangler()

	ts = time()
	while True:
		ts_now = time()
		# data = list((name, timestamp, val, val_raw), ...)
		data = list(it.starmap(
			ft.partial(mangler.process_host, ts=ts_now),
			gmond_xml_process(gmond_poll( optz.sources,
				libc_gethostbyname=optz.bypass_libc_gethostbyname )) ))

		log.debug('Publishing {} datapoints'.format(len(data)))
		if not optz.dry_run:
			raise NotImplementedError
			amqp.publish(data)

		while ts < ts_now: ts += optz.interval
		ts_sleep = max(0, ts - time())
		log.debug('Sleep: {}s'.format(ts_sleep))
		sleep(ts_sleep)

if __name__ == '__main__': main()
