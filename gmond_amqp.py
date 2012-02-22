#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
from gevent import Greenlet, socket
import gevent

import itertools as it, operator as op, functools as ft
from contextlib import closing
from collections import Iterable, deque
import os, sys, logging, types


def gmond_poll( sources,
		to_escalate=None, to_break=None, cycle=cycle,
		src_escalate=[1, 1, 2.0], libc_gethostbyname=True ):
	'''XML with values is fetched from possibly-multiple sources,
			first full dump received is returned.
		sources: iterable of sources to query - either hostname/ip or tuple of (hostname/ip, port)
		src_escalate:
			# number of sources to query simultaneously in the beginning and add after each to_escalate
			int - how many sources to query after each to_escalate passes,
			float (0-1.0) - percentage of sources,
			or iterable of ints/floats - value to use for each step, last one being used for the rest
		to_escalate: # timeout before starting querying additional sources
			int/float or iterable of these
		to_break: int/float # timeout to stop waiting for data for one source (break connection)
		cycle: int/float # used to calculate sensible values for to_*, if none is specified'''

	# Otherwise gevent does it's own (although parallel)
	#  gethostbyname, ignoring libc (ldap, nis, /etc/hosts), which is wrong
	# Obvious downside, is that it's serial - i.e. all hosts will be resolved here and now,
	#  before any actual xml fetching takes place, can be delayed but won't suck any less
	libc_gethostbyname = gethostbyname if libc_gethostbyname else lambda x: x
	sources = list(( (libc_gethostbyname(src), 8649)
		if isinstance(src, types.StringTypes) else libc_gethostbyname(src) ) for src in sources)

	# First calculate number of escalation tiers, then pick proper intervals
	log = logging.getLogger('gmond_amqp.poller')
	src_escalate = deque( src_escalate
		if isinstance(src_escalate, Iterable) else [src_escalate] )
	src_slice, src_count = src_escalate.popleft(), len(sources)
	src_tiers = list()
	while sources:
		src_tier, sources = sources[:src_slice], sources[src_slice:]
		src_tiers.append(src_tier)
		src_slice = src_escalate.popleft()
		if isinstance(src_slice, float): src_slice = int(src_count / src_slice)
	log.debug('Source tiers: {}'.format(src_tiers))

	if to_escalate is None:
		to_escalate = [1, ((cycle - 1) / 2.0) / len(src_tiers)] # so they'll fit in half-cycle
	if not isinstance(to_escalate, Iterable): to_escalate = [to_escalate]
	if to_break is None: to_break = cycle

	def fetch_from_src(source):
		with gevent.Timeout(to_break),\
				closing(socket.socket(
					socket.AF_INET, socket.SOCK_STREAM )) as sock:
			sock.connect(source)
			buff = bytes()
			while True:
				chunk = sock.recv(1*2**20)
				if not chunk: break
				buff += chunk
			return buff

	src_tiers, to_escalate = (list(reversed(q)) for q in [src_tiers, to_escalate])
	queries, result = Group(), AsyncResult()
	try:
		while src_tiers:
			src_tier, to = src_tiers.pop(), to_escalate.pop()
			for src in src_tier:
				queries.spawn(fetch_from_src, src).link(result)
			try: return result.get(block=True, timeout=to)
			except gevent.Timeout: pass
	finally: queries.kill()



def main():
	import argparse
	parser = argparse.ArgumentParser(
		description='Collect various metrics from gmond and dispatch'
			' them graphite-style at regular intervals to amqp (so they can be routed to carbon).')
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

	sources = 'raptor.v4c', 'apocrypha.v4c'
	print(gmond_poll(sources, libc_gethostbyname=optz.bypass_libc_gethostbyname))

if __name__ == '__main__': main()
