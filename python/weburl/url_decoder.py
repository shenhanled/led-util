#!/usr/bin/env python
# -*- coding: utf-8 -*-

import urllib

class UrlDecoder(object):

    def decode(self, line, delim='\t', cols=None):
        fields = line.split(delim)
        for idx in range(len(fields)):
            if (cols is not None) and (idx not in cols):
                continue

            fields[idx] = urllib.unquote(fields[idx])

        return delim.join(fields)
