#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
Compare two objects with rounding.

Developed at: 
The National Center for Supercomputing Applications (NCSA).

Copyright (C) 2012 Board of Trustees of the University of Illinois. 
All rights reserved.
"""

__version__ = "$Rev$"

from decimal import Decimal
from pprint  import pformat

def compare_rounded (item_a, item_b, places = 7):
    """
    Check two items for near equality.

    Return a tuple, the first element of which is a Boolean indicating
    whether the input meets the near equality constraint while the second
    element is a possibly nested structure containing a description of any
    discrepancies.  Differences for sequences and mappings are reported in
    dictionaries with keys indicating the entries with differences and
    values indicating the differences.  Differences for other types are
    reported as a string.

    Keys of mappings are compared, extra keys reported, and values of
    matching keys compared recursively.  Entries in sequences are compared
    in order and extra entries reported.  Other types are compared directly
    and then, if they differ, the difference is computed and rounded to the
    specified number of places if possible with nonzero results or failures
    to compute reported as a difference.
    """

    errors = _compare_items (item_a, item_b, places)
    if errors:
        return False, errors
    else:
        return True, None

def _compare_items (item_a, item_b, places):
    """
    Return None if the items match or a report of differences otherwise.
    
    For maps and sequences, call other functions.  For other types, compare
    the items.  If they differ, attempt to computer a difference and round
    it to the specified number of places.  Report a failure to compute or
    a nonzero result as a difference.
    """

    if hasattr (item_a, 'keys') and hasattr (item_b, 'keys'):
        return _compare_mappings (item_a, item_b, places)
    elif hasattr (item_a, '__iter__') and hasattr (item_b, '__iter__'):
        return _compare_sequences (item_a, item_b, places)
    elif item_a != item_b:
        try:
            # Python doesn't seem to support subtraction between a Decimal and
            # a float, so check for that and convert the float to Decimal.
            if type (item_a) == Decimal and type (item_b) == float: 
                i_a = item_a
                i_b = Decimal (item_b)
            elif type (item_a) == float and type (item_b) == Decimal:
                i_a = Decimal (item_a)
                i_b = item_b
            else:
                i_a = item_a
                i_b = item_b

            if round (i_a - i_b, places) != 0:
                raise TypeError ()  # Don't repeat the exception handler code
        except TypeError:
            return '%s != %s' % (pformat (item_a), pformat (item_b))

    return None

def _compare_sequences (seq_a, seq_b, places):
    """
    Recursively compare entries in the two sequences.

    Return a possibly empty report of extra entries and/or individual entry
    differences.
    """

    errors = {}
    entry_num = 0

    for item_a, item_b in zip (seq_a, seq_b):
        item_errors = _compare_items (item_a, item_b, places)
        if item_errors:
            errors ['Entry %d differs' % entry_num] = item_errors

        entry_num += 1

    long_seq = None
    if len (seq_a) < len (seq_b):
        long_seq = seq_b
        key  = 'Extra entries in B'
    if len (seq_a) > len (seq_b):
        long_seq = seq_a
        key  = 'Extra entries in A'

    if long_seq is not None:
        extras = []
        enum = 0
        for item in long_seq:
            if enum >= entry_num:
                extras.append (item)
            enum += 1
        errors [key] = extras

    return errors

def _compare_mappings (map_a, map_b, places):
    """
    Recursively compare entries in the two mappings.
    
    Return a possibly empty report of extra keys and/or individual key
    differences.
    """

    keys_a = set (map_a.keys ())
    keys_b = set (map_b.keys ())

    errors = {}

    extra_a = keys_a - keys_b
    if extra_a:
        errors ['Extra keys in A'] = sorted (list (extra_a))

    extra_b = keys_b - keys_a
    if extra_b:
        errors ['Extra keys in B'] = sorted (list (extra_b))

    for key in keys_a & keys_b:
        item_errors = _compare_items (map_a [key], map_b [key], places)

        if item_errors:
            errors ['Key "%s" differs' % key] = item_errors

    return errors
