#!/usr/bin/env python
# -*- coding: gbk -*-

import sys

class MapReduceJob(object):

    def __init__(self):
        self.mapred_class_dict = {}

    def register(self, name, mapred_class):
        self.mapred_class_dict[name] = mapred_class

    def run(self, params):
        if not isinstance(params, list):
            raise TypeError('params should be list')

        mapred_class_name = params[0]
        mapred_func_name = params[1]
        mapred_class_params = []
        if len(params) > 2:
            mapred_class_params = params[2:]

        mapred_class = self.mapred_class_dict[mapred_class_name]
        mapred_obj = mapred_class(params)

        if mapred_func_name == 'map':
            mapred_obj.map()
        elif mapred_func_name == 'reduce':
            mapred_obj.reduce()
        else:
            raise ValueError('unknown mapred_func_name: %s' % mapred_func_name)
