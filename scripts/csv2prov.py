#!/usr/bin/env python
"""Upload a csv file as RDF to a SPARQL triplestore
"""

import json
import os
from uuid import uuid1

import pandas as pd
import numpy as np
import prov.model as prov

def safe_encode(x):
    """Encodes a python value for prov
    """
    if x is None:
        return prov.Literal("Unknown", prov.XSD['string'])
    if isinstance(x, (str, unicode)):
        return prov.Literal(x, prov.XSD['string'])
    if isinstance(x, (int,)):
        return prov.Literal(int(x), prov.XSD['integer'])
    if isinstance(x, (float,)):
        return prov.Literal(x, prov.XSD['float'])
    return pm.Literal(json.dumps(x), pm.XSD['string'])

def csv2provgraph(filename, n_rows=None):
    """
    filename: path to file
    namespace: prov.Namespace instance to map column names to
    n_rows: number of rows to process
    """
    nidm = prov.Namespace('nidm', 'http://nidm.nidash.org/#')
    niiri = prov.Namespace('niiri', 'http://nidm.nidash.org/iri/')

    # create a new graph
    g = prov.ProvBundle()
    g.add_namespace(nidm)
    g.add_namespace(niiri)

    # uuid method
    get_id = lambda : uuid1().hex

    # csv prov:collection
    csv_id = get_id()
    csv_collection = g.collection(niiri[csv_id])
    csv_collection.add_extra_attributes({prov.PROV['type']: nidm['csv_file'],
                                         prov.PROV['label']: filename}
                                       )

    data = pd.read_csv(filename, na_values=["N/A", "pending", -999])

    columns = data.keys()
    column_collection = g.collection(niiri[get_id()])
    column_collection.add_extra_attributes({prov.PROV['type']: nidm['column_headers']})
    g.hadMember(csv_collection, column_collection)

    column_uri = {}
    for col_id, column in enumerate(columns):
        column_uri[column] = niiri[csv_id + '/' + column.rstrip().replace(' ', '_').replace('/', '_')]
        column_entity = g.entity(column_uri[column])
        column_entity.add_extra_attributes({prov.PROV['type']: nidm['csv_heading'],
                                            prov.PROV['label']: safe_encode(column),
                                            prov.PROV['location']: col_id})
        g.hadMember(column_collection, column_entity)

    row_count = 0
    for row in data.iterrows():
        if n_rows and row_count >= n_rows:
            break
        row_count +=1
        row_id = niiri[get_id()]
        # each row is an entity
        row_entity = g.entity(row_id)
        attr = {prov.PROV['type']: nidm['csv_row'],
                prov.PROV['location']: row_count}
        g.hadMember(csv_collection, row_id)
        for column in columns:
            if not np.isnan(row[1][column]):
                attr[column_uri[column]] = safe_encode(row[1][column])
        row_entity.add_extra_attributes(attr)
    return g

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

def convert_and_upload(filename):

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(prog='csv2prov.py',
                                     description=__doc__)
    parser.add_argument('-u', '--url', type=str, required=True,
                        help='Path to csv file')
    parser.add_argument('-e', '--endpoint', type=str,
                        help='SPARQL endpoint to use for update')
    parser.add_argument('-g', '--graph_iri', type=str,
                        help='Graph IRI to store the triples')

    args = parser.parse_args()
    if args.output_dir is None:
        args.output_dir = os.getcwd()

    graph = csv2provgraph(args.url)
    upload_graph(graph, endpoint=args.endpoint, uri=args.graph_iri)
