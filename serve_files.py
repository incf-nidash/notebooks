#!/usr/bin/env python
#
# Demo to return a file with a uri delivered by a virtuoso server

import md5
import json
import os
from urlparse import urlparse

import cherrypy
from nipype.utils.filemanip import hash_infile

FILE_DIR = os.path.join(os.getcwd(), 'files')

class FileServer(object):

    @cherrypy.expose
    def index(self):
        return "FileServer for triple store files"

    @cherrypy.expose
    def file(self, file_uri=None):
        print "File: ", file_uri
        parsed_object = urlparse(file_uri)
        fullpath = os.path.realpath(parsed_object.path)
        print fullpath
        if not os.path.exists(fullpath) or \
            not fullpath.startswith('/mindhive/xnat/surfaces/adhd200'):
            raise cherrypy.HTTPError("403 Forbidden", "You are not allowed to access this resource.")
        file_hash = hash_infile(fullpath)
        object_hash = md5.md5(file_uri + file_hash).hexdigest()
        link_path = os.path.join(FILE_DIR, object_hash)
        if not os.path.exists(link_path):
            os.symlink(fullpath, link_path)
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return json.dumps({'uri': cherrypy.url('/files/%s' % object_hash),
                           'md5sum': file_hash})

if not os.path.exists(FILE_DIR):
    os.mkdir(FILE_DIR)
config = {'/files': {'tools.staticdir.on': True,
                     'tools.staticdir.dir': FILE_DIR},
         }
cherrypy.config.update({'server.socket_host': '0.0.0.0',
                        'server.socket_port': 10101,
                       })
cherrypy.tree.mount(FileServer(), '/', config=config)
cherrypy.engine.start()
cherrypy.engine.block()
