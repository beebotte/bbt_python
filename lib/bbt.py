"""Python library for interfacing with Beebotte.

Contains methods for sending persistent and transient messages and for reading data.
"""

__version__ = '0.1.0'

import json
import hmac
import base64
import hashlib
import http.client
import urllib.parse
from email import utils

__publicReadEndpoint__ = "/api/public/resource"
__readEndpoint__       = "/api/resource/read"
__writeEndpoint__      = "/api/resource/write"
__bulkWriteEndpoint__  = "/api/resource/bulk_write"
__eventEndpoint__      = "/api/event/write"

class BBT:
  def __init__(self, akey, skey, hostname = "api.beebotte.com", port = "80", ssl = False):
    self.akey     = akey
    self.skey     = skey
    self.hostname = hostname
    self.port     = port
    self.ssl      = ssl

  def sign(self, stringToSign):
    signature = hmac.new(str.encode(self.skey), str.encode(stringToSign), hashlib.sha1)
    return "%s:%s" % (self.akey, bytes.decode(base64.b64encode(signature.digest())))

  def __signRequest__(self, verb, uri, date, c_type, c_md5 = ""):
    stringToSign = "%s\n%s\n%s\n%s\n%s" % (verb, c_md5, c_type, date, uri)
    return self.sign(stringToSign)

  def __postData__(self, uri, body, auth = True):
    # Set cURL options
    if self.ssl:
      conn = http.client.HTTPSConnection( self.hostname, self.port )
    else:
      conn = http.client.HTTPConnection( self.hostname, self.port )

    md5 = bytes.decode( base64.b64encode( hashlib.md5( str.encode( body ) ).digest() ) )
    date = utils.formatdate()
    if auth:
      sig = self.__signRequest__("POST", uri, date, "application/json", md5)
      conn.request('POST', uri, body, { 'Content-MD5': md5, 'Content-Type': 'application/json', 'Date': date, 'Authorization': sig } )
    else:
      conn.request('POST', uri, body, { 'Content-MD5': md5, 'Content-Type': 'application/json', 'Date': date } )
    
    resp = conn.getresponse()
    return { 'status': resp.status, 'body': resp.read() }

  def __getData__(self, uri, query, auth = True):
    # Set cURL options
    if self.ssl:
      conn = http.client.HTTPSConnection( self.hostname, self.port )
    else:
      conn = http.client.HTTPConnection( self.hostname, self.port )

    full_uri = "%s?%s" % ( uri, urllib.parse.urlencode( query ) )
    
    date = utils.formatdate()
    
    if auth:
      sig = self.__signRequest__('GET', full_uri, date, "application/json")
      conn.request('GET', full_uri, '', { 'Content-Type': 'application/json', 'Date': date, 'Authorization': sig } )
    else:
      conn.request('GET', full_uri, '', { 'Content-Type': 'application/json', 'Date': date } )
    
    resp = conn.getresponse()
    return { 'status': resp.status, 'body': resp.read() }

  def publicRead(self, owner, device, service, resource, limit = 1, source = "live", metric = "avg" ):
    query = {'owner': owner, 'device': device, 'service': service, 'resource': resource, 'limit': limit, 'source': source, 'metric': metric}
    
    response = self.__getData__( __publicReadEndpoint__, query, False )
    return response;

  def read(self, device, service, resource, limit = 1, source = "live", metric = "avg" ):
    query = { 'device': device, 'service': service, 'resource': resource, 'limit': limit, 'source': source, 'metric': metric }
    
    response = self.__getData__( __readEndpoint__, query, True )
    return response;

  def write(self, device, service, resource, value, type = "attribute" ):
    body = { 'device': device, 'service': service, 'resource': resource, 'value': value, 'type': type }
    
    response = self.__postData__( __writeEndpoint__, json.dumps(body, separators=(',', ':')), True )
    return response;

  def bulkWrite(self, device, data_array ):
    body = { 'device': device, 'data': data_array }
    
    response = self.__postData__( __bulkWriteEndpoint__, json.dumps(body, separators=(',', ':')), True )
    return response;

  def event(self, device, service, resource, data, source = None ):
    body = { 'device': device, 'service': service, 'resource': resource, 'data': data }
    if source:
      body['source'] = source
    
    response = self.__postData__( __eventEndpoint__, json.dumps(body, separators=(',', ':')), True )
    return response;

  def auth_client( self, sid, device, service = '*', resource = '*', ttl = 0, read = False, write = False ):
    r = 'false'
    w = 'false'
    if read:
      r = 'true'
    if write:
      w = 'true'
    stringToSign = "%s:%s.%s.%s:ttl=%s:read=%s:write=%s" % ( sid, device, service, resource, ttl, r, w )
    return self.sign(stringToSign)

class AuthenticationError(Exception):
    pass

class NotFoundError(Exception):
    pass

class UsageLimitError(Exception):
    pass

class UnexpectedError(Exception):
    pass

class BadRequestError(Exception):
    pass
