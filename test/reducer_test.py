#!/usr/bin/python

import copy
import random
import sys

# prepend the location of the local python-lounge
# the tests will find the local copy first, so we don't
# have to install system-wide before running tests.
sys.path = ['..'] + sys.path

from unittest import TestCase, main

from smartproxy import reducer

class ReducerTest(TestCase):
	def testMergeCollation(self):
		r1 = {'rows': [{'value': 1, 'key': 'a'}]}
		r2 = {'rows': [{'value': 6, 'key': 'a'}, {'value': 1, 'key': 'A'}]}

		# merge mangles its argument, so make a copy
		a = copy.deepcopy(r1)
		b = copy.deepcopy(r2)
		x = reducer.merge(b, a)
		self.assertEqual(len(x['rows']), 3)
		self.assertEqual(x['rows'][0]['key'], 'a')
		self.assertEqual(x['rows'][1]['key'], 'a')
		self.assertEqual(x['rows'][2]['key'], 'A')

		a = copy.deepcopy(r1)
		b = copy.deepcopy(r2)
		x = reducer.merge(a, b)
		self.assertEqual(len(x['rows']), 3)
		self.assertEqual(x['rows'][0]['key'], 'a')
		self.assertEqual(x['rows'][1]['key'], 'a')
		self.assertEqual(x['rows'][2]['key'], 'A')
	
	def testCollation(self):
		# reference list is taken from http://wiki.apache.org/couchdb/View_collation
		reference = [
			None, False, True,
			1, 2, 3.1, 4,
			"a", "A", "aa", "b", "B", "ba", "bb",
			["a"], ["b"], ["b","c"], ["b","c","a"], ["b","d"], ["b","d","e"],
			{"a": 1}, {"a": 2}, {"b": 1}, {"b": 2}, {"b": 2, "c": 2}]

		# make a shuffled copy of reference
		work = copy.deepcopy(reference)
		shuffle = []
		while work:
			shuffle.append(work.pop(random.randint(0,len(work)-1)))

		shuffle.sort(reducer.json_cmp)
		self.assertEqual(reference, shuffle)

if __name__=="__main__":
	main()
