#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

####################

cycle = 60 # minimal collection cycle, s

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
import os, sys, logging, types


class DataPollError(Exception): pass

def gmond_poll( sources,
		timeout=cycle, to_escalate=None, to_break=None,
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
	sources = list(( (libc_gethostbyname(src), 8649)
		if isinstance(src, types.StringTypes) else libc_gethostbyname(src) ) for src in sources)

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



def main():
	import argparse
	parser = argparse.ArgumentParser(
		description='Collect various metrics from gmond and dispatch'
			' them graphite-style at regular intervals to amqp (so they can be routed to carbon).')
	parser.add_argument('sources', nargs='+',
		help='Hostnames/ips (":port" optional, ipv4 only) of gmond nodes to poll.')
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

	print(gmond_poll(optz.sources, libc_gethostbyname=optz.bypass_libc_gethostbyname))

if __name__ == '__main__': main()
