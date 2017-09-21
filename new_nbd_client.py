#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (C) 2013 Nodalink, SARL.
#
# Simple nbd client used to connect to qemu-nbd
#
# author: Benoît Canet <benoit.canet@irqsave.net>
#
# This work is open source software, licensed under the terms of the
# BSD license as described in the LICENSE file in the top-level directory.
#

# Original file from
# https://github.com/cloudius-systems/osv/blob/master/scripts/nbd_client.py ,
# added support for (non-fixed) newstyle negotation,
# then @thomasmck added support for fixed-newstyle negotiation and TLS

import socket
import struct
import ssl


class new_nbd_client(object):

    # Request types
    NBD_CMD_READ = 0
    NBD_CMD_WRITE = 1
    # a disconnect request
    NBD_CMD_DISC = 2
    NBD_CMD_FLUSH = 3

    # Transmission flags
    NBD_FLAG_HAS_FLAGS = (1 << 0)
    NBD_FLAG_SEND_FLUSH = (1 << 2)

    # Client flags
    NBD_FLAG_C_FIXED_NEWSTYLE = (1 << 0)

    # Option types
    NBD_OPT_EXPORT_NAME = 1
    NBD_OPT_STARTTLS = 5

    # Option reply types
    NBD_REP_ACK = 1

    OPTION_REPLY_MAGIC = 0x3e889045565a9

    NBD_REQUEST_MAGIC = 0x25609513
    NBD_REPLY_MAGIC = 0x67446698

    def __init__(self, host, export_name="", ca_cert=None, port=10809):
        print("Connecting to export '{}' on host '{}'"
              .format(export_name, host))
        self._flushed = True
        self._closed = True
        self._handle = 0
        self._ca_cert = ca_cert
        self._s = socket.create_connection((host, port))
        self._closed = False
        self._fixed_new_style_handshake(export_name)

    def __del__(self):
        self.close()

    def close(self):
        if not self._flushed:
            self.flush()
        if not self._closed:
            self._disconnect()
            self._closed = True

    def _receive_all_data(self, data_length):
        data = bytes()
        while len(data) < data_length:
            data = data + self._s.recv(data_length - len(data))
        assert (len(data) == data_length)
        return data

    def _send_option(self, option, data=[]):
        print("NBD sending option header")
        data_length = len(data)
        print("option='%d' data_length='%d'" % (option, data_length))
        self._s.sendall(b'IHAVEOPT')
        header = struct.pack(">LL", option, data_length)
        self._s.sendall(header + data)
        self._option = option

    def _parse_option_reply(self):
        print("NBD parsing option reply")
        reply = self._s.recv(8 + 4 + 4)
        (magic, option, reply_type, data_length) = struct.unpack(
            ">QLLL", reply)
        print("NBD reply magic='%x' option='%d' reply_type='%d'" %
              (magic, option, reply_type))
        assert (magic == self.OPTION_REPLY_MAGIC)
        assert (option == self._option)
        assert (reply_type == self.NBD_REP_ACK)
        data = self._receive_all_data(data_length)
        return data

    def _upgrade_socket_to_TLS(self):
        # Forcing the client to use TLSv1_2
        context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        context.options &= ~ssl.OP_NO_TLSv1
        context.options &= ~ssl.OP_NO_TLSv1_1
        context.options &= ~ssl.OP_NO_SSLv2
        context.options &= ~ssl.OP_NO_SSLv3
        context.check_hostname = False
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cafile=self.ca_cert)
        cleartext_socket = self._s
        self._s = context.wrap_socket(
            cleartext_socket, server_side=False, do_handshake_on_connect=True)

    def _initiate_TLS_upgrade(self):
        # start TLS negotiation
        self._send_option(self.NBD_OPT_STARTTLS)
        # receive reply
        data = self._parse_option_reply()
        assert (len(data) == 0)

    def _fixed_new_style_handshake(self, export_name):
        nbd_magic = self._s.recv(len("NBDMAGIC"))
        assert (nbd_magic == b'NBDMAGIC')
        nbd_magic = self._s.recv(len("IHAVEOPT"))
        assert (nbd_magic == b'IHAVEOPT')
        buf = self._s.recv(2)
        self._flags = struct.unpack(">H", buf)[0]
        assert (self._flags & self.NBD_FLAG_HAS_FLAGS != 0)
        client_flags = self.NBD_FLAG_C_FIXED_NEWSTYLE
        client_flags = struct.pack('>L', client_flags)
        self._s.sendall(client_flags)

        if self._ca_cert:
            # start TLS negotiation
            self._initiate_TLS_upgrade()
            # upgrade socket to TLS
            self._upgrade_socket_to_TLS()

        # request export
        self._send_option(self.NBD_OPT_EXPORT_NAME, str.encode(export_name))

        # non-fixed newstyle negotiation: we get this if the server is willing
        # to allow the export
        buf = self._s.recv(8)
        self._size = struct.unpack(">Q", buf)[0]
        # ignore the transmission flags (& zeroes)
        self._s.recv(2 + 124)
        print("Connected")

    def _build_request_header(self, request_type, offset, length):
        print("NBD request offset=%d length=%d" % (offset, length))
        command_flags = 0
        header = struct.pack('>LHHQQL', self.NBD_REQUEST_MAGIC, command_flags,
                             request_type, self._handle, offset, length)
        return header

    def _parse_reply(self, data_length=0):
        print("NBD parsing response, data_length=%d" % data_length)
        reply = self._s.recv(4 + 4 + 8)
        (magic, errno, handle) = struct.unpack(">LLQ", reply)
        print("NBD response magic='%x' errno='%d' handle='%d'" % (magic, errno,
                                                                  handle))
        assert (magic == self.NBD_REPLY_MAGIC)
        assert (handle == self._handle)
        self._handle += 1
        data = self._receive_all_data(data_length=data_length)
        print("NBD response received data_length=%d bytes" % data_length)
        return (data, errno)

    def _check_value(self, name, value):
        if not value % 512:
            return
        raise ValueError("%s=%i is not a multiple of 512" % (name, value))

    def write(self, data, offset):
        print("NBD_CMD_WRITE")
        self._check_value("offset", offset)
        self._check_value("size", len(data))
        self._flushed = False
        header = self._build_request_header(self.NBD_CMD_WRITE, offset,
                                            len(data))
        self._s.sendall(header + data)
        (_, errno) = self._parse_reply()
        assert (errno == 0)
        return len(data)

    def read(self, offset, length):
        print("NBD_CMD_READ")
        self._check_value("offset", offset)
        self._check_value("length", length)
        header = self._build_request_header(self.NBD_CMD_READ, offset, length)
        self._s.sendall(header)
        (data, errno) = self._parse_reply(length)
        assert (errno == 0)
        return data

    def need_flush(self):
        if self._flags & self.NBD_FLAG_SEND_FLUSH != 0:
            return True
        else:
            return False

    def flush(self):
        print("NBD_CMD_FLUSH")
        if self.need_flush() is False:
            self._flushed = True
            return True
        header = self._build_request_header(self.NBD_CMD_FLUSH, 0, 0)
        self._s.sendall(header)
        (_, errno) = self._parse_reply()
        if not errno:
            self._flushed = True
        return errno == 0

    def _disconnect(self):
        print("NBD_CMD_DISC")
        header = self._build_request_header(self.NBD_CMD_DISC, 0, 0)
        self._s.sendall(header)

    def size(self):
        return self._size
