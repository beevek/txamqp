#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""
Utility code to translate between python objects and AMQP encoded data
fields.
"""

from cStringIO import StringIO
from struct import pack, calcsize, unpack
from decimal import Decimal

class EOF(Exception):
  pass

class Codec(object):

  def __init__(self, stream):
    self.stream = stream
    self.nwrote = 0
    self.nread = 0
    self.incoming_bits = []
    self.outgoing_bits = []

  def read(self, n):
    data = self.stream.read(n)
    if n > 0 and len(data) == 0:
      raise EOF()
    self.nread += len(data)
    return data

  def write(self, s):
    self.flushbits()
    self.stream.write(s)
    self.nwrote += len(s)

  def flush(self):
    self.flushbits()
    self.stream.flush()

  def flushbits(self):
    if len(self.outgoing_bits) > 0:
      bytes = []
      index = 0
      for b in self.outgoing_bits:
        if index == 0: bytes.append(0)
        if b: bytes[-1] |= 1 << index
        index = (index + 1) % 8
      del self.outgoing_bits[:]
      for byte in bytes:
        self.encode_octet(byte)

  def pack(self, fmt, *args):
    self.write(pack(fmt, *args))

  def unpack(self, fmt):
    size = calcsize(fmt)
    data = self.read(size)
    values = unpack(fmt, data)
    if len(values) == 1:
      return values[0]
    else:
      return values

  def encode(self, type, value):
    getattr(self, "encode_" + type)(value)

  def decode(self, type):
    return getattr(self, "decode_" + type)()

  # bit
  def encode_bit(self, o):
    if o:
      self.outgoing_bits.append(True)
    else:
      self.outgoing_bits.append(False)

  def decode_bit(self):
    if len(self.incoming_bits) == 0:
      bits = self.decode_octet()
      for i in range(8):
        self.incoming_bits.append(bits >> i & 1 != 0)
    return self.incoming_bits.pop(0)

  # boolean
  def encode_boolean(self, o):
    self.pack("!?", o)

  def decode_boolean(self):
    return self.unpack("!?")

  # short-short-int
  def encode_short_short_int(self, o):
    self.pack("!b", o)

  def decode_short_short_int(self):
    return self.unpack("!b")

  # short-short-uint
  def encode_short_short_uint(self, o):
    self.pack("!B", o)

  def decode_short_short_uint(self):
    return self.unpack("!B")

  # short-int
  def encode_short_int(self, o):
    self.pack("!h", o)

  def decode_short_int(self):
    return self.unpack("!h")

  # short-uint
  def encode_short_uint(self, o):
    self.pack("!H", o)

  def decode_short_uint(self):
    return self.unpack("!H")

  # long-int
  def encode_long_int(self, o):
    self.pack("!l", o)

  def decode_long_int(self):
    return self.unpack("!l")

  # long-uint
  def encode_long_uint(self, o):
    self.pack("!L", o)

  def decode_long_uint(self):
    return self.unpack("!L")

  # long-long-int
  def encode_long_long_int(self, o):
    self.pack("!q", o)

  def decode_long_long_int(self):
    return self.unpack("!q")

  # long-long-uint
  def encode_long_long_uint(self, o):
    self.pack("!Q", o)

  def decode_long_long_uint(self):
    return self.unpack("!Q")

  # float
  def encode_float(self, o):
    self.pack("!f", o)

  def decode_float(self):
    return self.unpack("!f")

  # double
  def encode_double(self, o):
    self.pack("!d", o)

  def decode_double(self):
    return self.unpack("!d")

  # decimal-value
  def encode_decimal_value(self, value):
    if isinstance(value, Decimal):
      value = value.normalize()
    else:
      raise ValueError("Uncompatible type")
    if value._exp < 0:
      decimals = -value._exp
      raw = int(value * (Decimal(10) ** decimals))
      self.pack("!c", "D")
      self.pack("!B", decimals)
      self.pack("!i", raw)
    else:
      # per spec, the "decimals" octet is unsigned (!)
      self.pack("!c", "D")
      self.pack("!B", 0)
      self.pack("!i", int(value))

  def decode_decimal_value(self):
    decimals = self.decode_short_short_uint()
    raw = self.decode_long_uint()
    return Decimal(raw) * (Decimal(10) ** -decimals)

  # short-string
  def encode_short_string(self, s):
    size = len(s)
    self.encode_short_short_uint(size)
    self.write(s)

  def decode_short_string(self):
    size = self.decode_short_short_uint()
    return self.read(size)

  # long-string
  def encode_long_string(self, s):
    size = len(s)
    self.encode_short_short_uint(size)
    self.write(s)

  def decode_long_string(self):
    size = self.decode_long_uint()
    return self.read(size)

  def field_value(self):
    type = self.read(1)
    if type == "t":
      return self.decode_boolean()
    elif type == "b":
      return self.decode_short_short_int()
    elif type == "B":
      return self.decode_short_short_uint()
    elif type == "U":
      return self.decode_short_int()
    elif type == "u":
      return self.decode_short_uint()
    elif type == "I":
      return self.decode_long_int()
    elif type == "i":
      return self.decode_long_uint()
    elif type == "L":
      return self.decode_long_long_int()
    elif type == "l":
      return self.decode_long_long_uint()
    elif type == "f":
      return self.decode_float()
    elif type == "d":
      return self.decode_double()
    elif type == "D":
      return self.decode_decimal_value()
    elif type == "s":
      return self.decode_short_string()
    elif type == "S":
      return self.decode_long_string()
    elif type == "A":
      return self.decode_field_array()
    elif type == "T":
      return self.decode_timestamp()
    elif type == "F":
      return self.decode_field_table()
    elif type == "V":
      return self.decode_none()
    else:
      raise ValueError("Unkown value type: %s" %repr(type))

  # field-array
  def decode_field_array(self):
    size = self.decode_long_int()
    start = self.nread
    result = []
    while self.nread - start < size:
      result.append(self.field_value())
    real_size = self.nread - start
    if real_size != size:
      raise ValueError("Problem with size of field array (%d != %d)" %
                       (real_size, size))
    return result

  # timestamp
  def encode_timestamp(self, o):
    self.encode_long_long_uint(o)

  def decode_timestamp(self):
    return self.decode_long_long_uint()

  # field-table
  def encode_table(self, tbl):
    self.encode_field_table(tbl)
  def encode_field_table(self, tbl):
    enc = StringIO()
    codec = Codec(enc)
    for key, value in tbl.items():
      codec.encode_shortstr(key)
      if isinstance(value, basestring):
        codec.write("S")
        codec.encode_longstr(value)
      else:
        codec.write("I")
        codec.encode_long(value)
    s = enc.getvalue()
    self.encode_long_uint(len(s))
    self.write(s)

  def decode_table(self):
    return self.decode_field_table()
  def decode_field_table(self):
    size = self.decode_long_uint()
    start = self.nread
    result = {}
    while self.nread - start < size:
      key = self.decode_short_string()
      result[key] = self.field_value()
    return result

  # none
  def encode_none(self, o):
    self.pack("!x", o)

  def decode_none(self):
    return self.unpack("!x")

  # octet
  def encode_octet(self, o):
    self.encode_short_short_uint(o)

  def decode_octet(self):
    return self.decode_short_short_uint()

  # short
  def encode_short(self, o):
    self.encode_short_uint(o)

  def decode_short(self):
    return self.decode_short_uint()

  # long
  def encode_long(self, o):
    self.encode_long_uint(o)

  def decode_long(self):
    return self.decode_long_uint()

  # longlong
  def encode_longlong(self, o):
    self.encode_long_long_uint(o)

  def decode_longlong(self):
    return self.decode_long_long_uint()

  def enc_str(self, fmt, s):
    size = len(s)
    self.pack(fmt, size)
    self.write(s)

  def dec_str(self, fmt):
    size = self.unpack(fmt)
    return self.read(size)

  # shortstr
  def encode_shortstr(self, s):
    self.encode_short_string(s)

  def decode_shortstr(self):
    return self.decode_short_string()

  # longstr
  def encode_longstr(self, s):
    if isinstance(s, dict):
      self.encode_table(s)
    else:
      self.enc_str("!L", s)

  def decode_longstr(self):
    return self.dec_str("!L")


def test(type, value):
  if isinstance(value, (list, tuple)):
    values = value
  else:
    values = [value]
  stream = StringIO()
  codec = Codec(stream)
  for v in values:
    codec.encode(type, v)
  codec.flush()
  enc = stream.getvalue()
  stream.reset()
  dup = []
  for i in xrange(len(values)):
    dup.append(codec.decode(type))
  if values != dup:
    raise AssertionError("%r --> %r --> %r" % (values, enc, dup))

if __name__ == "__main__":
  def dotest(type, value):
    args = (type, value)
    test(*args)

  for value in ("1", "0", "110", "011", "11001", "10101", "10011"):
    for i in range(10):
      dotest("bit", map(lambda x: x == "1", value*i))

  for value in ({}, {"asdf": "fdsa", "fdsa": 1, "three": 3}, {"one": 1}):
    dotest("table", value)

  for type in ("octet", "short", "long", "longlong"):
    for value in range(0, 256):
      dotest(type, value)

  for type in ("shortstr", "longstr"):
    for value in ("", "a", "asdf"):
      dotest(type, value)
