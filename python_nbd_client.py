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

"""
A pure-Python NBD client.
"""

import socket
import struct
import ssl
import logging

# Request types
NBD_CMD_READ = 0
NBD_CMD_WRITE = 1
# a disconnect request
NBD_CMD_DISC = 2
NBD_CMD_FLUSH = 3
NBD_CMD_BLOCK_STATUS = 7

# Transmission flags
NBD_FLAG_HAS_FLAGS = (1 << 0)
NBD_FLAG_SEND_FLUSH = (1 << 2)

# Client flags
NBD_FLAG_C_FIXED_NEWSTYLE = (1 << 0)

# Option types
NBD_OPT_EXPORT_NAME = 1
NBD_OPT_ABORT = 2
NBD_OPT_STARTTLS = 5
NBD_OPT_STRUCTURED_REPLY = 8
NBD_OPT_LIST_META_CONTEXT = 9
NBD_OPT_SET_META_CONTEXT = 10

# Option reply types
NBD_REP_ERROR_BIT = (1 << 31)
NBD_REP_ACK = 1
NBD_REP_META_CONTEXT = 4

OPTION_REPLY_MAGIC = 0x3e889045565a9

NBD_REQUEST_MAGIC = 0x25609513
NBD_SIMPLE_REPLY_MAGIC = 0x67446698
NBD_STRUCTURED_REPLY_MAGIC = 0x668e33ef

# Structured reply types
NBD_REPLY_TYPE_NONE = 0
NBD_REPLY_OFFSET_DATA = 1
NBD_REPLY_TYPE_OFFSET_HOLE = 2
NBD_REPLY_TYPE_BLOCK_STATUS = 3
NBD_REPLY_TYPE_ERROR_BIT = (1 << 15)
NBD_REPLY_TYPE_ERROR = (1 << 15 + 1)
NBD_REPLY_TYPE_ERROR_OFFSET = (1 << 15 + 2)

# Structured reply flags
NBD_REPLY_FLAG_DONE = (1 << 0)


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
        super().__init__("Server returned error during transmission: {}"
                         .format(error_code))
        self.error_code = error_code


class NBDOptionError(Exception):
    """
    The NBD server replied with an error to the option sent by the client.

    :attribute reply: The error reply sent by the server.
    """
    def __init__(self, reply):
        super().__init__("Server returned error during option haggling: {}"
                         .format(reply))
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
        super().__init__("Received response to unexpected option {}; "
                         "was expecting a response to option {}"
                         .format(received, expected))
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
        super().__init__("Received reply with unexpected handle {}; "
                         "was expecting a response to the request with "
                         "handle {}"
                         .format(received, expected))
        self.expected = expected
        self.received = received


class NBDProtocolError(Exception):
    """
    The NBD server sent an invalid response that is not allowed by the NBD
    protocol.
    """
    pass


def _assert_protocol(assertion):
    if assertion is False:
        raise NBDProtocolError


def _check_alignment(name, value):
    if not value % 512:
        return
    raise ValueError("%s=%i is not a multiple of 512" % (name, value))


class PythonNbdClient(object):
    """
    A pure-Python NBD client. Supports both the fixed-newstyle and the
    oldstyle negotiation, and also has support for upgrading the
    connection to TLS during fixed-newstyle negotiation.
    """

    def __init__(self,
                 address,
                 exportname="",
                 port=10809,
                 timeout=60,
                 subject=None,
                 cert=None,
                 use_tls=True,
                 new_style_handshake=True,
                 unix=False,
                 connect=True):
        logging.info("Connecting to export '%s' on host '%s' and port '%s'",
                     exportname, address, port)
        self._flushed = True
        self._closed = True
        self._handle = 0
        self._last_sent_option = None
        self._structured_reply = False
        self._transmission_phase = False
        if unix:
            self._s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        else:
            self._s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if not unix:
            address = (address, int(port))
        self._s.settimeout(timeout)
        self._s.connect(address)
        self._closed = False
        if new_style_handshake:
            self._fixed_new_style_handshake(
                cert=cert,
                subject=subject,
                use_tls=use_tls)
            if connect:
                self.connect(exportname=exportname)
        else:
            self._old_style_handshake()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        """
        Sends a flush request to the server if necessary and the server
        supports it, followed by a disconnect request.
        """
        if not self._flushed:
            self.flush()
        if not self._closed:
            self._disconnect()
            self._closed = True

    def _recvall(self, length):
        data = bytearray(length)
        view = memoryview(data)
        bytes_left = length
        while bytes_left:
            received = self._s.recv_into(view, bytes_left)
            # If recv reads 0 bytes, that means the peer has properly
            # shut down the TCP session (end-of-file error):
            if not received:
                raise NBDEOFError
            view = view[received:]
            bytes_left -= received
        return data

    # Handshake phase

    #  Newstyle handshake

    def _send_option(self, option, data=b''):
        logging.debug("NBD sending option header")
        data_length = len(data)
        logging.debug("option='%d' data_length='%d'", option, data_length)
        self._s.sendall(b'IHAVEOPT')
        header = struct.pack(">LL", option, data_length)
        self._s.sendall(header + data)
        self._last_sent_option = option

    def _parse_option_reply(self):
        logging.debug("NBD parsing option reply")
        reply = self._recvall(8 + 4 + 4 + 4)
        (magic, option, reply_type, data_length) = struct.unpack(
            ">QLLL", reply)
        logging.debug("NBD reply magic='%x' option='%d' reply_type='%d'",
                      magic, option, reply_type)
        _assert_protocol(magic == OPTION_REPLY_MAGIC)
        if option != self._last_sent_option:
            raise NBDUnexpectedOptionResponseError(
                expected=self._last_sent_option, received=option)
        if reply_type & NBD_REP_ERROR_BIT != 0:
            raise NBDOptionError(reply=reply_type)
        data = self._recvall(data_length)
        return (reply_type, data)

    def _parse_option_reply_ack(self):
        (reply_type, data) = self._parse_option_reply()
        if reply_type != NBD_REP_ACK:
            raise NBDProtocolError()
        return data

    def _parse_meta_context_reply(self):
        (reply_type, data) = self._parse_option_reply()
        if reply_type == NBD_REP_ACK:
            return None
        (context_id) = struct.unpack(">L", data[:4])
        name = str(data[4:], encoding='utf-8')
        return (context_id, name)

    def _upgrade_socket_to_tls(self, cert, subject):
        # Forcing the client to use TLSv1_2
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.options &= ~ssl.OP_NO_TLSv1
        context.options &= ~ssl.OP_NO_TLSv1_1
        context.options &= ~ssl.OP_NO_SSLv2
        context.options &= ~ssl.OP_NO_SSLv3
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = (subject is not None)
        context.load_verify_locations(cadata=cert)
        cleartext_socket = self._s
        self._s = context.wrap_socket(
            cleartext_socket,
            server_side=False,
            do_handshake_on_connect=True,
            server_hostname=subject)

    def _initiate_tls_upgrade(self):
        # start TLS negotiation
        self._send_option(NBD_OPT_STARTTLS)
        # receive reply
        data = self._parse_option_reply_ack()
        _assert_protocol(len(data) == 0)

    def negotiate_structured_reply(self):
        """
        Negotiate use of the structured reply extension, fail if unsupported.
        Only valid during the handshake phase.
        """
        self._send_option(NBD_OPT_STRUCTURED_REPLY)
        self._parse_option_reply_ack()
        self._structured_reply = True

    def _send_meta_context_option(self, option, export_name, queries):
        data = bytes()
        data += struct.pack('>L', len(export_name))
        data += export_name.encode('utf-8')
        data += struct.pack('>L', len(queries))
        for query in queries:
            data += struct.pack('>L', len(query))
            data += query.encode('utf-8')
        self._send_option(option, data)
        while True:
            reply = self._parse_meta_context_reply()
            if reply is None:
                break
            yield reply

    def set_meta_contexts(self, export_name, queries):
        """
        Change the set of active metadata contexts. Only valid during the
        handshake phase.
        """
        self._send_meta_context_option(
            option=NBD_OPT_SET_META_CONTEXT,
            export_name=export_name,
            queries=queries)

    def list_meta_contexts(self, export_name, queries):
        """
        Return the metadata contexts available on the export matching one or
        more of the queries as (metadata context ID, metadata context name)
        pairs.
        """
        self._send_meta_context_option(
            option=NBD_OPT_LIST_META_CONTEXT,
            export_name=export_name,
            queries=queries)

    def _fixed_new_style_handshake(self, cert, subject, use_tls):
        nbd_magic = self._recvall(len("NBDMAGIC"))
        _assert_protocol(nbd_magic == b'NBDMAGIC')
        nbd_magic = self._recvall(len("IHAVEOPT"))
        _assert_protocol(nbd_magic == b'IHAVEOPT')
        buf = self._recvall(2)
        self._flags = struct.unpack(">H", buf)[0]
        _assert_protocol(self._flags & NBD_FLAG_HAS_FLAGS != 0)
        client_flags = NBD_FLAG_C_FIXED_NEWSTYLE
        client_flags = struct.pack('>L', client_flags)
        self._s.sendall(client_flags)

        if use_tls:
            # start TLS negotiation
            self._initiate_tls_upgrade()
            # upgrade socket to TLS
            self._upgrade_socket_to_tls(cert, subject)

    def connect(self, exportname):
        """
        Valid only during the handshake phase. Requests the given
        export and enters the transmission phase."""
        # request export
        self._send_option(NBD_OPT_EXPORT_NAME, str.encode(exportname))

        # non-fixed newstyle negotiation: we get this if the server is willing
        # to allow the export
        buf = self._recvall(8)
        self._size = struct.unpack(">Q", buf)[0]
        # ignore the transmission flags (& zeroes)
        transmission_flags = self._recvall(2)
        logging.debug("NBD got transmission flags: %s", transmission_flags)
        zeroes = self._recvall(124)
        self._transmission_phase = True
        logging.debug("NBD got zeroes: %s", zeroes)
        logging.debug("Connected")

    #  Oldstyle handshake

    def _old_style_handshake(self):
        nbd_magic = self._recvall(len("NBDMAGIC"))
        _assert_protocol(nbd_magic == b'NBDMAGIC')
        buf = self._recvall(8 + 8 + 4)
        (magic, self._size, self._flags) = struct.unpack(">QQL", buf)
        _assert_protocol(magic == 0x00420281861253)
        # ignore trailing zeroes
        self._recvall(124)

    # Transmission phase

    def _send_request_header(self, request_type, offset, length):
        logging.debug("NBD request offset=%d length=%d", offset, length)
        command_flags = 0
        self._handle += 1
        header = struct.pack('>LHHQQL', NBD_REQUEST_MAGIC, command_flags,
                             request_type, self._handle, offset, length)
        self._s.sendall(header)

    def _check_handle(self, handle):
        if handle != self._handle:
            raise NBDUnexpectedReplyHandleError(
                expected=self._handle, received=handle)

    def _parse_simple_reply(self, data_length=0):
        logging.debug("NBD parsing simple reply, data_length=%d", data_length)
        reply = self._recvall(4 + 4 + 8)
        (magic, errno, handle) = struct.unpack(">LLQ", reply)
        logging.debug("NBD simple reply magic='%x' errno='%d' handle='%d'",
                      magic, errno, handle)
        _assert_protocol(magic == NBD_SIMPLE_REPLY_MAGIC)
        self._check_handle(handle)
        data = self._recvall(length=data_length)
        logging.debug("NBD response received data_length=%d bytes", data_length)
        if errno != 0:
            raise NBDTransmissionError(errno)
        return data

    def _handle_block_status_reply(self, fields):
        data = self._recvall(fields['data_length'])
        view = memoryview(data)
        fields['context_id'] = struct.unpack(">L", view[:4])
        view = view[4:]
        descriptors = []
        while view:
            (length, status_flags) = struct.unpack(">LL", view[:8])
            descriptors += [(length, status_flags)]
            view = view[8:]
        fields['descriptors'] = descriptors

    def _handle_structured_reply_error(self, fields):
        buf = self._recvall(4 + 2)
        (errno, message_length) = struct.unpack(">LH", buf)
        fields['error'] = errno
        remaining_length = fields['data_length'] - 6
        if message_length > remaining_length:
            # message_length is too large to fit within data_length bytes
            raise NBDProtocolError
        data = self._recvall(remaining_length)
        view = memoryview(data)
        fields['message'] = str(view[0:message_length], encoding='utf-8')
        view = view[message_length:]
        if fields['reply_type'] == NBD_REPLY_TYPE_ERROR_OFFSET:
            (offset) = struct.unpack(">Q", view[:8])
            fields['offset'] = offset

    def _parse_structured_reply_chunk(self, read_magic=True):
        logging.debug("NBD parsing structured reply chunk")
        if read_magic:
            magic = self._recvall(4)
            logging.debug("NBD structured reply magic='%x'", magic)
            _assert_protocol(magic == NBD_STRUCTURED_REPLY_MAGIC)
        reply = self._recvall(2 + 2 + 8 + 4)
        (flags, reply_type, handle, data_length) = struct.unpack(">HHQL", reply)
        logging.debug("NBD structured reply flags='%s' reply_type='%d' handle='%d' data_length='%d'",
                      flags, reply_type, handle, data_length)
        self._check_handle(handle)
        fields = {'flags': flags, 'reply_type': reply_type, 'data_length': data_length}
        if reply_type == NBD_REPLY_TYPE_BLOCK_STATUS:
            self._handle_block_status_reply(fields)
        if reply_type & NBD_REPLY_TYPE_ERROR_BIT != 0:
            self._handle_structured_reply_error(fields)
        return fields

    def _parse_structured_reply_chunks(self, read_first_magic=True):
        reply = self._parse_structured_reply_chunk(read_first_magic)
        while True:
            yield reply
            if reply['flags'] & NBD_REPLY_FLAG_DONE == NBD_REPLY_FLAG_DONE:
                return
            reply = self._parse_structured_reply_chunk()

    def write(self, data, offset):
        """
        Writes the given bytes to the export, starting at the given
        offset.
        """
        logging.debug("NBD_CMD_WRITE")
        _check_alignment("offset", offset)
        _check_alignment("size", len(data))
        self._flushed = False
        self._send_request_header(NBD_CMD_WRITE, offset, len(data))
        self._s.sendall(data)
        self._parse_simple_reply()
        return len(data)

    def read(self, offset, length):
        """
        Returns length number of bytes read from the export, starting at
        the given offset.
        """
        logging.debug("NBD_CMD_READ")
        _check_alignment("offset", offset)
        _check_alignment("length", length)
        self._send_request_header(NBD_CMD_READ, offset, length)
        data = self._parse_simple_reply(length)
        return data

    def _need_flush(self):
        return self._flags & NBD_FLAG_SEND_FLUSH != 0

    def flush(self):
        """
        Sends a flush request to the server if the server supports it
        and there are unflushed writes. This causes all completed writes
        (the writes for which the server has already sent a reply to the
        client) to be written to permanent storage.
        """
        logging.debug("NBD_CMD_FLUSH")
        if self._need_flush() is False:
            self._flushed = True
            return True
        self._send_request_header(NBD_CMD_FLUSH, 0, 0)
        self._parse_simple_reply()
        self._flushed = True

    def query_block_status(self, offset, length):
        """Query block status in the range defined by length and offset."""
        logging.debug("NBD_CMD_READ")
        self._send_request_header(NBD_CMD_BLOCK_STATUS, offset, length)
        return self._parse_structured_reply_chunks()

    def _disconnect(self):
        if self._transmission_phase:
            logging.debug("NBD_CMD_DISC")
            self._send_request_header(NBD_CMD_DISC, 0, 0)
        else:
            self._send_option(NBD_OPT_ABORT)

    def get_size(self):
        """
        Return the size of the device in bytes.
        """
        return self._size
