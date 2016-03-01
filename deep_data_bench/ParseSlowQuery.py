#!/usr/bin/env python2.7
import argparse
import json

from metadata import ParseSlowQueryFile

if __name__ == '__main__':
	config_items = {'file':			{'value':None}}
	parser = argparse.ArgumentParser()
	for k, v in sorted(config_items.iteritems()):
			parser.add_argument("--" + k)
	args = parser.parse_args()
	for k, v in config_items.iteritems():
		if eval(str("args."+k)) == None:
			if 'default' in config_items[k].keys():
				config_items[k]['value'] = config_items[k]['default']
			else: 
				config_items[k]['value'] = raw_input("Enter " + k + ":")
		else:
			config_items[k]['value'] = eval(str("args."+k))

		tt = ParseSlowQueryFile(config_items['file']['value'])
		tt.printQueryTypeCount()
