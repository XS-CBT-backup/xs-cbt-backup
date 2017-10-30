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


class NBDEOFError(EOFError):
    """
    An end of file error happened while reading from the socket, because it has
    been closed.
    """
    pass


class NBDTransmissionError(Exception):
    """
    The NBD server returned a non-zero error value in its response to a
    request.

    :attribute error_code: The error code returned by the server.
    """
    def __init__(self, error_code):
        self.error_code = error_code


class NBDOptionError(Exception):
    """
    The NBD server replied with an error to the option sent by the client.

    :attribute reply: The error reply sent by the server.
    """
    def __init__(self, reply):
        self.reply = reply


class NBDUnexpectedOptionResponseError(Exception):
    """
    The NBD server sent a response to an option different from the most recent
    one that the client is expecting a response to.

    :attribute expected: The option that was last sent by the client, to which
                         it is expecting a response.
    :attribute received: The server's response is a reply to this option.
    """
    def __init__(self, expected, received):
        self.expected = expected
        self.received = received


class NBDUnexpectedReplyHandleError(Exception):
    """
    The NBD server sent a reply to a request different from the most recent one
    that the client is expecting a response to.

    :attribute expected: The handle of the most recent request that the client
                         is expecting a reply to.
    :attribute received: The server's reply contained this handle.
    """
    def __init__(self, expected, received):
        self.expected = expected
        self.received = received


class NBDProtocolError(Exception):
    """
    The NBD server sent an invalid response that is not allowed by the NBD
    protocol.
    """
    pass


def assert_protocol(b):
    if b is False:
        raise NBDProtocolError


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

    def __init__(self,
                 address,
                 exportname="",
                 port=10809,
                 subject=None,
                 cert=None,
                 use_tls=True,
                 new_style_handshake=True):
        print("Connecting to export '{}' on host '{}' and port '{}'"
              .format(exportname, address, port))
        self._flushed = True
        self._closed = True
        self._handle = 0
        self._cert = cert
        self._subject = subject
        self._s = socket.create_connection(address=(address, port), timeout=30)
        self._closed = False
        if new_style_handshake:
            self._fixed_new_style_handshake(
                exportname=exportname, use_tls=use_tls)
        else:
            self._old_style_handshake()

    def __del__(self):
        self.close()

    def close(self):
        if not self._flushed:
            self.flush()
        if not self._closed:
            self._disconnect()
            self._closed = True

    def _recvall(self, length):
        data = bytes()
        while len(data) < length:
            b = self._s.recv(length - len(data))
            if (len(b) == 0):
                raise NBDEOFError
            data = data + b
        assert len(data) == length
        return data

    def _send_option(self, option, data=b''):
        print("NBD sending option header")
        data_length = len(data)
        print("option='%d' data_length='%d'" % (option, data_length))
        self._s.sendall(b'IHAVEOPT')
        header = struct.pack(">LL", option, data_length)
        self._s.sendall(header + data)
        self._option = option

    def _parse_option_reply(self):
        print("NBD parsing option reply")
        reply = self._recvall(8 + 4 + 4 + 4)
        (magic, option, reply_type, data_length) = struct.unpack(
            ">QLLL", reply)
        print("NBD reply magic='%x' option='%d' reply_type='%d'" %
              (magic, option, reply_type))
        assert_protocol(magic == self.OPTION_REPLY_MAGIC)
        if (option != self._option):
            raise NBDUnexpectedOptionResponseError(
                expected=self._option, received=option)
        if reply_type != self.NBD_REP_ACK:
            raise NBDOptionError(reply=reply_type)
        data = self._recvall(data_length)
        return data

    def _upgrade_socket_to_TLS(self):
        # Forcing the client to use TLSv1_2
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.options &= ~ssl.OP_NO_TLSv1
        context.options &= ~ssl.OP_NO_TLSv1_1
        context.options &= ~ssl.OP_NO_SSLv2
        context.options &= ~ssl.OP_NO_SSLv3
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = (self._subject is not None)
        context.load_verify_locations(cadata=self._cert)
        cleartext_socket = self._s
        self._s = context.wrap_socket(
            cleartext_socket,
            server_side=False,
            do_handshake_on_connect=True,
            server_hostname=self._subject)

    def _initiate_TLS_upgrade(self):
        # start TLS negotiation
        self._send_option(self.NBD_OPT_STARTTLS)
        # receive reply
        data = self._parse_option_reply()
        assert_protocol(len(data) == 0)

    def _fixed_new_style_handshake(self, exportname, use_tls):
        nbd_magic = self._recvall(len("NBDMAGIC"))
        assert_protocol(nbd_magic == b'NBDMAGIC')
        nbd_magic = self._recvall(len("IHAVEOPT"))
        assert_protocol(nbd_magic == b'IHAVEOPT')
        buf = self._recvall(2)
        self._flags = struct.unpack(">H", buf)[0]
        assert_protocol(self._flags & self.NBD_FLAG_HAS_FLAGS != 0)
        client_flags = self.NBD_FLAG_C_FIXED_NEWSTYLE
        client_flags = struct.pack('>L', client_flags)
        self._s.sendall(client_flags)

        if use_tls:
            # start TLS negotiation
            self._initiate_TLS_upgrade()
            # upgrade socket to TLS
            self._upgrade_socket_to_TLS()

        # request export
        self._send_option(self.NBD_OPT_EXPORT_NAME, str.encode(exportname))

        # non-fixed newstyle negotiation: we get this if the server is willing
        # to allow the export
        buf = self._recvall(8)
        self._size = struct.unpack(">Q", buf)[0]
        # ignore the transmission flags (& zeroes)
        transmission_flags = self._recvall(2)
        print("NBD got transmission flags: {}".format(transmission_flags))
        zeroes = self._recvall(124)
        print("NBD got zeroes: {}".format(zeroes))
        print("Connected")

    def _old_style_handshake(self):
        nbd_magic = self._recvall(len("NBDMAGIC"))
        assert_protocol(nbd_magic == b'NBDMAGIC')
        buf = self._recvall(8 + 8 + 4)
        (magic, self._size, self._flags) = struct.unpack(">QQL", buf)
        assert_protocol(magic == 0x00420281861253)
        # ignore trailing zeroes
        self._recvall(124)

    def _build_request_header(self, request_type, offset, length):
        print("NBD request offset=%d length=%d" % (offset, length))
        command_flags = 0
        header = struct.pack('>LHHQQL', self.NBD_REQUEST_MAGIC, command_flags,
                             request_type, self._handle, offset, length)
        return header

    def _parse_reply(self, data_length=0):
        print("NBD parsing response, data_length=%d" % data_length)
        reply = self._recvall(4 + 4 + 8)
        (magic, errno, handle) = struct.unpack(">LLQ", reply)
        print("NBD response magic='%x' errno='%d' handle='%d'" % (magic, errno,
                                                                  handle))
        assert_protocol(magic == self.NBD_REPLY_MAGIC)
        if handle != self._handle:
            raise NBDUnexpectedReplyHandleError(
                expected=self._handle, received=handle)
        self._handle += 1
        data = self._recvall(length=data_length)
        print("NBD response received data_length=%d bytes" % data_length)
        if errno != 0:
            raise NBDTransmissionError(errno)
        return data

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
        self._parse_reply()
        return len(data)

    def read(self, offset, length):
        print("NBD_CMD_READ")
        self._check_value("offset", offset)
        self._check_value("length", length)
        header = self._build_request_header(self.NBD_CMD_READ, offset, length)
        self._s.sendall(header)
        data = self._parse_reply(length)
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
        self._parse_reply()
        self._flushed = True

    def _disconnect(self):
        print("NBD_CMD_DISC")
        header = self._build_request_header(self.NBD_CMD_DISC, 0, 0)
        self._s.sendall(header)

    def size(self):
        return self._size
