#!/usr/bin/env python

import math
import calendar

def func_getnite(dateobs):
    v = dateobs.split(':')
    hh = int(v[0].split('-')[2][-2:])
    if hh > 14:
        nite = v[0][:-3].replace('-','')   
    else:
        y = int(v[0][0:4])
        m = int(v[0][5:7])
        d = int(v[0][8:10])-1
        if d==0:
            m = m - 1
            if m==0:
                m = 12
                y = y - 1
            d = calendar.monthrange(y,m)[1]
        nite = str(y).zfill(4)+str(m).zfill(2)+str(d).zfill(2)
    return nite


def func_getband(filter):
    band = filter[0]
    if band not in ['u','g','r','i','z','Y']:
        raise KeyError("filter yeilds invalid band")
    return band

