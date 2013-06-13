#!/usr/bin/env python
"""Query for FreeSurfer stat files, encode using NI-DM and upload
"""

#standard library
from datetime import datetime as dt
import hashlib
import os
from tempfile import mktemp
import pwd
import urlparse
import urllib
from uuid import uuid1

# PROV API library
import pandas as pd
import prov.model as prov
import rdflib
import requests

def get_collections(endpoint, limit=1000):
    """Get all freesurfer subject directory collections from remote endpoint
    """
    query = """
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX fs: <http://freesurfer.net/fswiki/terms/>
    PREFIX crypto: <http://www.w3.org/2000/10/swap/crypto#>
    PREFIX nidm: <http://nidm.nidash.org/terms/>
    select ?collection where
    {?collection a prov:Collection;
                 a fs:subject_directory .
    }
    LIMIT %d
    """ % limit
    g = rdflib.Graph('SPARQLStore')
    g.open(endpoint)
    results = g.query(query)
    return results

def get_urls(endpoint, collection, limit=1000):
    query = """
    PREFIX prov: <http://www.w3.org/ns/prov#>
    PREFIX fs: <http://freesurfer.net/fswiki/terms/>
    PREFIX crypto: <http://www.w3.org/2000/10/swap/crypto#>
    PREFIX nidm: <http://nidm.nidash.org/terms/>
    select ?e ?relpath ?md5 ?path where
    {<%s> a prov:Collection;
        prov:hadMember ?e .
     ?e fs:FileType fs:statistic_file;
        fs:relative_path ?relpath;
        crypto:md5 ?md5;
        prov:location ?path .
     FILTER NOT EXISTS {
      ?e nidm:tag "curv" .
     }
     FILTER NOT EXISTS {
      ?out prov:wasDerivedFrom ?e;
           a prov:Collection .
     }
    }
    LIMIT %d
    """ % (collection, limit)
    g = rdflib.Graph('SPARQLStore')
    g.open(endpoint)
    results = g.query(query)
    return results

def read_stats(filename):
    """Convert stats file to a structure
    """
    header = {}
    tableinfo = {}
    measures = []
    rowmeasures = []
    with open(filename, 'rt') as fp:
        lines = fp.readlines()
        for line in lines:
            if line == line[0]:
                continue
            #parse commented header
            if line.startswith('#'):
                fields = line.split()[1:]
                if len(fields) < 2:
                    continue
                tag = fields[0]
                if tag == 'TableCol':
                    col_idx = int(fields[1])
                    if col_idx not in tableinfo:
                        tableinfo[col_idx] = {}
                    tableinfo[col_idx][fields[2]] = ' '.join(fields[3:])
                    if tableinfo[col_idx][fields[2]] == "StructName":
                        struct_idx = col_idx
                elif tag == "Measure":
                    fields = ' '.join(fields[1:]).split(', ')
                    measures.append({'structure': fields[0],
                                     'name': fields[1],
                                     'description': fields[2],
                                     'value': fields[3],
                                     'units': fields[4],
                                     'source': 'Header'})
                elif tag == "ColHeaders":
                    continue
                else:
                    header[tag] = ' '.join(fields[1:])
            else:
                #read values
                row = line.split()
                values = {}
                measures.append({'structure': row[struct_idx-1],
                                 'items': [],
                                 'source': 'Table'}),
                for idx, value in enumerate(row):
                    if idx + 1 == struct_idx:
                        continue
                    measures[-1]['items'].append({
                        'name': tableinfo[idx + 1]['ColHeader'],
                        'description': tableinfo[idx + 1]['FieldName'],
                        'value': value,
                        'units': tableinfo[idx + 1]['Units'],
                        })
    return header, tableinfo, measures

def parse_stats(fs_stat_file, entity_uri):
    """Convert stats file to a nidm object
    """
    foaf = prov.Namespace("foaf", "http://xmlns.com/foaf/0.1/")
    dcterms = prov.Namespace("dcterms", "http://purl.org/dc/terms/")
    fs = prov.Namespace("fs", "http://freesurfer.net/fswiki/terms/")
    nidm = prov.Namespace("nidm", "http://nidm.nidash.org/terms/")
    niiri = prov.Namespace("niiri", "http://nidm.nidash.org/iri/")
    obo = prov.Namespace("obo", "http://purl.obolibrary.org/obo/")
    nif = prov.Namespace("nif", "http://neurolex.org/wiki/")
    crypto = prov.Namespace("crypto", "http://www.w3.org/2000/10/swap/crypto#")

    header, tableinfo, measures = read_stats(fs_stat_file)
    g = prov.ProvBundle()
    
    # Set the default _namespace name
    #g.set_default_namespace(fs.get_uri())
    g.add_namespace(foaf)
    g.add_namespace(dcterms)
    g.add_namespace(fs)
    g.add_namespace(nidm)
    g.add_namespace(niiri)

    get_id = lambda : niiri[uuid1().hex]
    a0 = g.activity(get_id(), startTime=dt.isoformat(dt.utcnow()))
    user_agent = g.agent(get_id(),
                         {prov.PROV["type"]: prov.PROV["Person"],
                          prov.PROV["label"]: pwd.getpwuid(os.geteuid()).pw_name,
                          foaf["name"]: pwd.getpwuid(os.geteuid()).pw_name})
    g.wasAssociatedWith(a0, user_agent, None, None,
                        {prov.PROV["Role"]: "LoggedInUser"})
    stat_collection = g.collection(get_id())
    stat_collection.add_extra_attributes({prov.PROV['type']: nidm['FreeSurferStatsCollection']})
    # header elements
    statheader_collection = g.entity(get_id())
    attributes = {prov.PROV['type']: fs['stat_header']}
    for key, value in header.items():
        attributes[fs[key]] = value
    statheader_collection.add_extra_attributes(attributes)
    # measures
    struct_info = {}
    measure_list = []
    measure_graph = rdflib.ConjunctiveGraph()
    measure_graph.namespace_manager.bind('fs', fs.get_uri())
    measure_graph.namespace_manager.bind('nidm', nidm.get_uri())
    unknown_units = set(('unitless', 'NA'))
    for measure in measures:
        obj_attr = []
        struct_uri = fs[measure['structure']]
        if measure['source'] == 'Header':
            measure_name = measure['name']
            if measure_name not in measure_list:
                measure_list.append(measure_name)
                measure_uri = fs[measure_name].rdf_representation()
                measure_graph.add((measure_uri,
                                   rdflib.RDF['type'],
                                   fs['Measure'].rdf_representation()))
                measure_graph.add((measure_uri,
                                   rdflib.RDFS['label'],
                                   rdflib.Literal(measure['description'])))
                measure_graph.add((measure_uri,
                                   nidm['units'].rdf_representation(),
                                   rdflib.Literal(measure['units'])))
            obj_attr.append((nidm["AnatomicalAnnotation"], struct_uri))
            if str(measure['units']) in unknown_units:
                valref = prov.Literal(int(measure['value']), prov.XSD['integer'])
            else:
                valref= prov.Literal(float(measure['value']), prov.XSD['float'])
            obj_attr.append((fs[measure_name], valref))
        elif measure['source'] == 'Table':
            obj_attr.append((nidm["AnatomicalAnnotation"], struct_uri))
            for column_info in measure['items']:
                measure_name = column_info['name']
                if column_info['units'] in unknown_units and \
                   '.' not in column_info['value']:
                    valref = prov.Literal(int(column_info['value']),
                                          prov.XSD['integer'])
                else:
                    valref= prov.Literal(float(column_info['value']),
                                         prov.XSD['float'])
                obj_attr.append((fs[measure_name], valref))
                if measure_name not in measure_list:
                    measure_list.append(measure_name)
                    measure_uri = fs[measure_name].rdf_representation()
                    measure_graph.add((measure_uri,
                                       rdflib.RDF['type'],
                                       fs['Measure'].rdf_representation()))
                    measure_graph.add((measure_uri,
                                       rdflib.RDFS['label'],
                                       rdflib.Literal(column_info['description'])))
                    measure_graph.add((measure_uri,
                                       nidm['units'].rdf_representation(),
                                       rdflib.Literal(column_info['units'])))
        id = get_id()
        if struct_uri in struct_info:
            euri = struct_info[struct_uri]
            euri.add_extra_attributes(obj_attr)
        else:
            euri = g.entity(id, obj_attr)
            struct_info[struct_uri] = euri
        g.hadMember(stat_collection, id)
    g.hadMember(stat_collection, statheader_collection)
    g.derivation(stat_collection, entity_uri)
    g.wasGeneratedBy(stat_collection, a0)
    return g, measure_graph

def job(row):
    entity, relpath, md5sum, urlget = row[0], row[1], row[2], row[3]
    r = requests.get(urlget).json()
    if str(md5sum) == str(r['md5sum']):
        filename = mktemp()
        urllib.urlretrieve(r['uri'], filename)
        stats_graph, measure_graph = parse_stats(filename, entity)
        os.unlink(filename)
        return stats_graph, measure_graph
    return None

def upload_graph(graph, endpoint=None, uri='http://test.nidm.org'):
    import requests
    from requests.auth import HTTPDigestAuth

    # connection params for secure endpoint
    if endpoint is None:
        endpoint = 'http://bips.incf.org:8890/sparql'

    # session defaults
    session = requests.Session()
    session.headers = {'Accept': 'text/html'}  # HTML from SELECT queries

    counter = 0
    max_stmts = 1000
    stmts = graph.rdf().serialize(format='nt').splitlines()
    N = len(stmts)
    while (counter < N):
        endcounter = min(N, counter + max_stmts)
        query = """
        INSERT IN GRAPH <%s>
        {
        %s
        }
        """ % (uri, '\n'.join(stmts[counter:endcounter]))
        data = {'query': query}
        result = session.post(endpoint, data=data)
        print(result)
        counter = endcounter
    print('Submitted %d statemnts' % N)

def process_collection(endpoint, collection, graph_iri):
    results = get_urls(endpoint, collection)
    for row in results:
        g, _ = job(row)
        #mg.parse('fsterms.ttl', format='turtle')
        #mg.serialize('fsterms.ttl', format='turtle')
        upload_graph(g, endpoint=endpoint, uri=graph_iri)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(prog='query_convert_fs_stats.py',
                                     description=__doc__)
    parser.add_argument('-e', '--endpoint', type=str,
                        help='SPARQL endpoint to use for update')
    parser.add_argument('-g', '--graph_iri', type=str,
                        help='Graph IRI to store the triples')
    parser.add_argument('-o', '--output_dir', type=str,
                        help='Output directory')
    parser.add_argument('-c', '--collection', type=str,
                        help='Identifier for collection')

    args = parser.parse_args()
    if args.output_dir is None:
        args.output_dir = os.getcwd()

    #process_collection(args.endpoint, args.collection, args.graph_iri)
    #graph = to_graph(args.subject_dir, args.project_id, args.output_dir,
    #                 args.hostname)
    #upload_graph(graph, endpoint=args.endpoint, uri=args.graph_iri)
