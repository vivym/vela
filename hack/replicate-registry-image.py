#!/usr/bin/env python3
"""Copy an exact OCI image/index and verify every destination blob by SHA256.

Preserves nested image manifests and attestations; never flattens a multi-platform
index. Only the three approved internal registry hosts are accepted. New content
is appended by digest, without deleting images or changing existing tags. Optional
credentials are read from a private JSON file, never command arguments or stdout.
"""
import argparse
import base64
import datetime
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import ssl
import urllib.parse

TYPES=['application/vnd.oci.image.index.v1+json','application/vnd.docker.distribution.manifest.list.v2+json',
       'application/vnd.oci.image.manifest.v1+json','application/vnd.docker.distribution.manifest.v2+json']
HOSTS={'10.1.201.70','10.1.201.71','10.1.201.66'}


class Registry:
    def __init__(self,host,context,auth=None,timeout=30):
        if host not in HOSTS:raise ValueError('unapproved registry host')
        self.host=host;self.context=context;self.auth=auth;self.timeout=timeout

    def connection(self):
        return http.client.HTTPSConnection(self.host,5005,context=self.context,timeout=self.timeout)

    def headers(self,extra=None):
        headers={'Accept':', '.join(TYPES)}
        if self.auth:headers['Authorization']=self.auth
        headers.update(extra or {})
        return headers

    def request(self,method,path,body=None,headers=None):
        connection=self.connection()
        connection.request(method,path,body=body,headers=self.headers(headers))
        return connection,connection.getresponse()

    def fetch(self,method,path,body=None,headers=None):
        connection,response=self.request(method,path,body,headers)
        try:return response.status,dict(response.headers),response.read()
        finally:connection.close()

    def location(self,location):
        url=urllib.parse.urlsplit(urllib.parse.urljoin('https://'+self.host+':5005/',location))
        if url.scheme!='https' or url.hostname!=self.host or url.port!=5005 or not url.path.startswith('/v2/'):
            raise RuntimeError('registry returned an unexpected upload destination')
        return urllib.parse.urlunsplit(('', '', url.path, url.query, ''))


def digest(data):return 'sha256:'+hashlib.sha256(data).hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source',default='10.1.201.70',choices=sorted(HOSTS))
    parser.add_argument('--target',action='append',required=True,choices=sorted(HOSTS))
    parser.add_argument('--repository',required=True)
    parser.add_argument('--digest',required=True)
    parser.add_argument('--ca',default='/etc/rancher/rke2/registry-ca.crt')
    parser.add_argument('--credentials')
    parser.add_argument('--user',default='platform-publisher')
    parser.add_argument('--receipt',required=True)
    parser.add_argument('--timeout',type=float,default=30,
                        help='socket timeout in seconds, including server-side blob digest verification (max 900)')
    args=parser.parse_args()
    if not math.isfinite(args.timeout) or not 0 < args.timeout <= 900:parser.error('timeout must be finite and between 0 and 900 seconds')
    if not re.fullmatch(r'[a-z0-9]+(?:[._/-][a-z0-9]+)*',args.repository):parser.error('invalid repository')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}',args.digest):parser.error('invalid digest')
    if args.source in args.target:parser.error('source cannot be a target')
    os.umask(0o077)
    context=ssl.create_default_context(cafile=args.ca)
    auth=None
    if args.credentials:
        path=Path(args.credentials)
        if path.stat().st_mode & 0o077:parser.error('credential file must be private')
        password=json.loads(path.read_text())[args.user]
        auth='Basic '+base64.b64encode((args.user+':'+password).encode()).decode()
    source=Registry(args.source,context,auth,args.timeout)
    prefix='/v2/'+args.repository
    receipt={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':args.source,
             'repository':args.repository,'root_digest':args.digest,'socket_timeout_seconds':args.timeout,'targets':[]}
    cache={}

    def manifest(ref,expected_size=None):
        if ref not in cache:
            code,headers,body=source.fetch('GET',prefix+'/manifests/'+ref)
            if code!=200:raise RuntimeError('source manifest GET failed: '+str(code))
            media=headers['Content-Type'].split(';')[0]
            if media not in TYPES or digest(body)!=ref:raise RuntimeError('source manifest media type or digest differs')
            value=json.loads(body)
            if value.get('schemaVersion')!=2:raise RuntimeError('unsupported manifest schema')
            cache[ref]=(media,body,value)
        if expected_size is not None and len(cache[ref][1])!=expected_size:raise RuntimeError('manifest descriptor size differs')
        return cache[ref]

    try:
        for host in args.target:
            target=Registry(host,context,auth,args.timeout)
            outcome={'host':host,'manifests':[],'blobs':[]};receipt['targets'].append(outcome)
            seen=set();blobs=set()
            def copy_blob(descriptor):
                ref=descriptor['digest'];size=descriptor['size']
                if not re.fullmatch(r'sha256:[0-9a-f]{64}',ref) or size<0:raise RuntimeError('invalid blob descriptor')
                if ref in blobs:return
                path=prefix+'/blobs/'+ref
                code,_,_=target.fetch('HEAD',path)
                created=False
                if code==404:
                    source_conn,source_response=source.request('GET',path)
                    if source_response.status!=200 or int(source_response.getheader('Content-Length','-1'))!=size:
                        source_conn.close();raise RuntimeError('source blob status or size differs')
                    code,headers,_=target.fetch('POST',prefix+'/blobs/uploads/',b'')
                    if code!=202:source_conn.close();raise RuntimeError('upload initiation failed')
                    upload=target.location(headers['Location'])
                    connection=target.connection()
                    try:
                        destination=upload+('&' if '?' in upload else '?')+'digest='+ref
                        connection.putrequest('PUT',destination)
                        for key,value in target.headers({'Content-Type':'application/octet-stream','Content-Length':str(size)}).items():
                            connection.putheader(key,value)
                        connection.endheaders()
                        hashed=hashlib.sha256();count=0
                        while chunk:=source_response.read(1024*1024):
                            count+=len(chunk);hashed.update(chunk);connection.send(chunk)
                        response=connection.getresponse();status=response.status;response.read()
                        if status!=201 or count!=size or 'sha256:'+hashed.hexdigest()!=ref:
                            raise RuntimeError('blob upload or source SHA256 verification failed')
                        created=True
                    finally:
                        source_conn.close();connection.close()
                        if not created:
                            target.fetch('DELETE',upload)
                elif code!=200:raise RuntimeError('destination blob HEAD failed: '+str(code))
                connection,response=target.request('GET',path)
                try:
                    hashed=hashlib.sha256();count=0
                    if response.status!=200:raise RuntimeError('destination verification GET failed')
                    while chunk:=response.read(1024*1024):hashed.update(chunk);count+=len(chunk)
                    if count!=size or 'sha256:'+hashed.hexdigest()!=ref:raise RuntimeError('destination blob SHA256 differs')
                finally:connection.close()
                blobs.add(ref);outcome['blobs'].append({'digest':ref,'size':size,'uploaded':created,'sha256_verified':True})
            def copy_manifest(ref,size=None):
                if ref in seen:return
                media,body,value=manifest(ref,size)
                if media in TYPES[:2]:
                    for child in value['manifests']:copy_manifest(child['digest'],child['size'])
                else:
                    copy_blob(value['config'])
                    for layer in value['layers']:copy_blob(layer)
                code,_,existing=target.fetch('GET',prefix+'/manifests/'+ref)
                if code==404:
                    code,_,_=target.fetch('PUT',prefix+'/manifests/'+ref,body,{'Content-Type':media})
                    if code!=201:raise RuntimeError('destination manifest publication failed: '+str(code))
                elif code!=200 or existing!=body:raise RuntimeError('destination existing manifest differs')
                code,_,actual=target.fetch('GET',prefix+'/manifests/'+ref)
                if code!=200 or actual!=body:raise RuntimeError('destination exact manifest verification failed')
                seen.add(ref);outcome['manifests'].append({'digest':ref,'media_type':media,'size':len(body),'verified':True})
            copy_manifest(args.digest)
            outcome['result']='EXACT_IMAGE_REPLICA_PASS'
            print(json.dumps({'target':host,'result':outcome['result'],'manifests':len(seen),'blobs':len(blobs),
                              'verified_bytes':sum(b['size'] for b in outcome['blobs'])}),flush=True)
        receipt['result']='EXACT_IMAGE_REPLICATION_PASS'
    finally:
        Path(args.receipt).write_text(json.dumps(receipt,indent=2)+'\n')


if __name__=='__main__':main()
