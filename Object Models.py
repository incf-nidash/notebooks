# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <markdowncell>

# # NI-DM: A Summary in many parts
# 
# ## Part 1: Constructing and Querying Object Models
# 
# satra@mit.edu, nolan.nichols@gmail.com
# 
# [Latest version](https://github.com/INCF/ni-dm/tree/source/source/notebooks/Object models.ipynb)

# <markdowncell>

# ## Outline
# 
# - What is a data model, provenance, [PROV-DM](http://www.w3.org/TR/prov-dm/) and [NI-DM](http://nidm.nidash.org)?
# - What is an object model?
# - Using the [PROV Python library](https://github.com/trungdong/prov) to construct object models
# - Querying object models with [SPARQL](http://www.w3.org/TR/rdf-sparql-query/) using a [triplestore](http://en.wikipedia.org/wiki/Triplestore) 
# - Mapping [XCEDE](http://www.xcede.org/) "experiment/research" primitives to NI-DM

# <markdowncell>

# ## What is a data model?
# 
# A data model is an abstract conceptual formulation of information that explictly determines the structure of data and allows software and people to communicate and interpret data precisely. [ source: http://en.wikipedia.org/wiki/Data_model ]
# 
# ## What is Provenance?
# 
# Provenance is information about entities, activities, and people involved in producing a piece of data or thing, which can be used to form assessments about its quality, reliability or trustworthiness.
# 
# ## What is PROV-DM?
# 
# [PROV-DM](http://www.w3.org/TR/prov-dm/) is the conceptual data model that forms a basis for the W3C provenance (PROV) [family of specifications](http://www.w3.org/2011/prov/wiki/WorkingDrafts).
# 
# PROV-DM is organized in six components, respectively dealing with: (1) [entities and activities](http://www.w3.org/TR/prov-dm/#section-entity-activity), and the time at which they were created, used, or ended; (2) [derivations](http://www.w3.org/TR/prov-dm/#section-derivation) of entities from entities; (3) [agents](http://www.w3.org/TR/prov-dm/#section-agents-attribution-association-delegation) bearing responsibility for entities that were generated and activities that happened; (4) a notion of [bundle](http://www.w3.org/TR/prov-dm/#section-provenance-of-provnance), a mechanism to support provenance of provenance; (5) [properties to link entities](http://www.w3.org/TR/prov-dm/#section-prov-extended-mechanisms) that refer to the same thing; and, (6) [collections](http://www.w3.org/TR/prov-dm/#section-collections) forming a logical structure for its members.
# 
# ## What is NI-DM?
# 
# NI-DM is formulated as a domain specific extension of PROV-DM. Currently, NI-DM maps identically to PROV-DM and domain extensions are captured as object models on top of PROV-DM.

# <markdowncell>

# ### Basic Provenance data model
# 
# <img src="http://www.w3.org/TR/prov-o/diagrams/starting-points.svg" />

# <markdowncell>

# ### Extended provenance model
# 
# <img src="http://www.w3.org/TR/2013/PR-prov-o-20130312/diagrams/expanded.svg" />

# <markdowncell>

# ### What are the advantages of using NI-DM
# 
# - Provenance is not an afterthought
# - Terminology is important
# - W3C recommendation at this point
# - Many tools are starting to support the model
# - Captures data and metadata (about entities, **activities** and agents) within the same context
# - Simplifies app development
# - Maps to Semantic Web tools allowing query federation
# 
# ### What are the disadvantages of using NI-DM
# 
# - A new way of thinking about queries and computation
# - Requires extensive terminology
# - Current databases are not built for provenance
# - Current databases are very specifically structured
# - The transition will require resources, commitment and time

# <markdowncell>

# ## What is an object model?
# 
# An object model represents a collection "through which a program can examine and manipulate some specific parts of its world."
# 
# ## What are object models in NI-DM?
# 
# In the context of NI-DM, object models capture specific relationships between [entities][entity] via [collections][collection] that reflect organization information derived from imaging files (e.g., DICOM, Nifti, MINC), directory structures (e.g., Freesurfer, OpenFMRI), phenotypic data (e.g., neuropsych assessments, csv files) and binary or text files (e.g., SPM.mat, Feat.fsf, aseg.stats). The models are associated with appropriate provenance.
# 
# [entity]: http://www.w3.org/TR/prov-o/#Entity
# [collection]: http://www.w3.org/TR/prov-o/#Collections

# <markdowncell>

# ## Using the PROV API to construct object models
# 
# We will demonstrate how to encapsulate FreeSurfer directory structures and statistic files, and CSV files using the PROV API.

# <markdowncell>

# ### Model 1: FreeSurfer Directory structure
# 
# The FreeSurfer output results from a recon-all process that involves several steps. The goal here is to take the directory structure and represent it as a collection of entities, where each entity is a file produced by the recon-all process. In the ideal mode, the recon-all process itself would generate the provenance and the collection of entities. But here we generate the NI-DM encoding of FreeSurfer directories by walking FreeSurfer directories and applying heuristic properties.
# 
# For demonstration purposes, we automatically generated terms based on file/directory naming conventions, and assossate these terms with an example FreeSurfer namespace [http://surfer.nmr.mgh.harvard.edu/fswiki/terms/0.1/]. This namespace is only a placeholder; however, the developers of a given neuroimaging software package (e.g., FreeSurfer) are responsible for managing a namespace that resolves to a vocabulary that describes their NI-DM object model. The best practice is to provide a URL accessible [RDF](http://www.w3.org/RDF/) representation of the terms in this namespace along with definitions for each term.
# 
# Some example terms are:
#     
#     - fs:subject_id : a participant identifier
#     - fs:relative_path : location of file from the root of the subject directory structure
# 
# note: `fs:` is a namespace prefix that allows for a shorthand notation of the full URI (e.g., `http://surfer.nmr.mgh.harvard.edu/fswiki/terms/0.1/subject_id`) 

# <markdowncell>

# #### Import necessary tools and create namespaces
# 
# This example uses two Python libraries that you will need to install
# 
#     - Nipype: http://nipy.org/nipype/
#     - PROV: https://github.com/trungdong/prov

# <codecell>

# standard library
import os
import md5
from socket import getfqdn

# PROV API library
import prov.model as prov

def hash_infile(afile, chunk_len=8192):
    """ Computes md5 hash of a file"""
    md5hex = None
    if os.path.isfile(afile):
        md5obj = md5.md5()
        fp = file(afile, 'rb')
        while True:
            data = fp.read(chunk_len)
            if not data:
                break
            md5obj.update(data)
        fp.close()
        md5hex = md5obj.hexdigest()
    return md5hex

# create namespace references to terms used
foaf = prov.Namespace("foaf", "http://xmlns.com/foaf/0.1/")
dcterms = prov.Namespace("dcterms", "http://purl.org/dc/terms/")
fs = prov.Namespace("fs", "http://surfer.nmr.mgh.harvard.edu/fswiki/terms/0.1/")
nidm = prov.Namespace("nidm", "http://nidm.nidash.org/terms/0.1/")
obo = prov.Namespace("obo", "http://purl.obolibrary.org/obo/")
nif = prov.Namespace("nif", "http://neurolex.org/wiki/")

# <markdowncell>

# ### Define a function that creates a NI-DM Entity for a freesurfer file

# <codecell>

ls -lR /Applications/freesurfer/subjects/bert

# <codecell>

# map FreeSurfer filename parts
fs_file_map =  [('T1', [nif["nlx_inv_20090243"]]), #3d T1 weighted scan
                ('lh', [obo["UBERON_0002812"]]), #left cerebral hemisphere
                ('rh', [obo["UBERON_0002813"]]), # right cerebral hemisphere
                ('BA.', [obo["UBERON_0013529"]]), #Brodmann area
                ('BA1.', [obo["UBERON_0006099"]]), #Brodmann area 1
                ('BA2.', [obo["UBERON_0013533"]]), #Brodmann area 2
                ('BA3a.', [obo["UBERON_0006100"], #Brodmann area 3a
                           obo["FMA_74532"]]), #anterior
                ('BA3b.', [obo["UBERON_0006100"], #Brodmann area 3b
                           obo["FMA_74533"]]), #posterior
                ('BA44.', [obo["UBERON_0006481"]]), #Brodmann area 44
                ('BA45.', [obo["UBERON_0006482"]]), #Brodmann area 45
                ('BA4a.', [obo["UBERON_0013535"], #Brodmann area 4a
                           obo["FMA_74532"]]), #anterior
                ('BA4a.', [obo["UBERON_0013535"], #Brodmann area 4p
                           obo["FMA_74533"]]), #posterior
                ('BA6.', [obo["UBERON_0006472"]]), #Brodmann area 6
                ('V1.', [obo["UBERON_0002436"]]),
                ('V2.', [obo["UBERON_0006473"]]),
                ('MT', [fs["MT_area"]]),
                ('entorhinal', [obo["UBERON_0002728"]]),
                ('exvivo', [fs["exvivo"]]),
                ('label', [fs["label_file"]]),
                ('annot', [fs["annotation_file"]]),
                ('cortex', [obo["UBERON_0000956"]]),
                ('.stats', [fs["statistic_file"]]),
                ('aparc.annot', [fs["default_parcellation"]]),
                ('aparc.a2009s', [fs["a2009s_parcellation"]]),
                ('.ctab', [fs["color_table"]]),
               ]

ignore_list = ['bak', 'src', 'tmp', 'trash']

# <codecell>

def create_entity(graph, fs_subject_id, filepath):
    """ Create a PROV entity for a file in a FreeSurfer directory
    """
    # identify FreeSurfer terms based on directory and file names
    _, filename = os.path.split(filepath)
    relpath = filepath.split(fs_subject_id)[1].lstrip(os.path.sep)
    fstypes = relpath.split('/')[:-1]
    additional_types = relpath.split('/')[-1].split('.')
    
    file_md5_hash = hash_infile(filepath)
    if file_md5_hash is None:
        print fullpath

    url = "file://%s%s" % (getfqdn(), filepath)
    url_get = "http://computor.mit.edu:10101/file?file_uri=%s" % url
    # build a NI-DM object 
    obj_attr = [(prov.PROV["label"], "%s:%s" % ('.'.join(fstypes), '.'.join(additional_types))),
                (prov.PROV["label"], "%s" % filename),
                (fs["relative_path"], "%s" % relpath),
                (prov.PROV["location"], prov.Literal(url, prov.XSD["anyURI"])),
                (obo["MS_1000568"], "%s" % file_md5_hash),
               ]

    for key, uris in fs_file_map:
        if key in filename:
            obj_attr.append((prov.PROV["label"], key.rstrip('.')))
            for uri in uris:
                obj_attr.append((prov.PROV["type"], uri))
    id = md5.new(fs_subject_id + relpath + file_md5_hash).hexdigest()
    return graph.entity(fs['e/' + id], obj_attr)
    #return id

# <codecell>

g = prov.ProvBundle()
e1 = create_entity(g, "bert", "/Applications/freesurfer/subjects/bert/mri/T1.mgz")

# <codecell>

id = e1.get_identifier()
print e1.get_provn()

# <markdowncell>

# ### Terms introduced
# 
#     - relative_path
#     - file
#     - md5sum
#     - aparc, a2005s, a2009s, exvivo
#     - statistics

# <markdowncell>

# ### Define a function that manages creating a NI-DM FreeSurfer object

# <codecell>

def encode_fs_directory(g, basedir, project_id, subject_id, n_items=100000):
    """ Convert a FreeSurfer directory to a PROV graph
    """
    # directory collection/catalog
    collection_hash = md5.new(project_id + ':' + subject_id).hexdigest()
    fsdir_collection = g.collection(fs[collection_hash])
    fsdir_collection.add_extra_attributes({prov.PROV['type']: fs['directory'],
                                           fs['subject_id']: subject_id})
    i = 0;
    for dirpath, dirnames, filenames in os.walk(os.path.realpath(basedir)):
        for filename in sorted(filenames):
            if filename.startswith('.'):
                continue
            i +=1 
            if i > n_items:
                break
            file2encode = os.path.realpath(os.path.join(dirpath, filename))
            if not os.path.isfile(file2encode):
                print "%s not a file" % file2encode
                continue
            ignore_key_found = False
            for key in ignore_list:
                if key in file2encode:
                    ignore_key_found = True
                    continue
            if ignore_key_found:
                continue
            try:
                entity = create_entity(g, subject_id, file2encode)
                g.hadMember(fsdir_collection, entity.get_identifier())
            except IOError, e:
                print e
    return g

# <markdowncell>

# ### Example of generating a NI-DM FreeSurfer Object
# 
# This example demonstrates the conversion from [PROV Notation](http://www.w3.org/TR/prov-n/) to RDF using the [ProvToolBox](https://github.com/lucmoreau/ProvToolbox), which provides a command line utility for serializing PROV to different syntaxes.

# <codecell>

# location of FreeSurfer $SUBJECTS_DIR
basedir = '/Applications/freesurfer/subjects/bert'
subject_id = 'bert'
project_id = 'nidm'
filename = 'bert.provn'

from subprocess import Popen, PIPE

# location of the ProvToolBox commandline conversion utility
pc = '/mindhive/gablab/satra/ProvToolbox/toolbox/target/appassembler/bin/provconvert'
graph = prov.ProvBundle()
graph.add_namespace(foaf)
graph.add_namespace(dcterms)
graph.add_namespace(fs)
graph.add_namespace(nidm)
graph.add_namespace(obo)
graph.add_namespace(nif)

graph = encode_fs_directory(graph, basedir, project_id, subject_id)
with open(filename, 'wt') as fp:
    fp.writelines(graph.get_provn())
#o, e = Popen('%s -infile %s -outfile %s.ttl' % (pc, filename, filename), 
#             stdout=PIPE, stderr=PIPE, shell=True).communicate()

# <codecell>

cat bert.provn

# <markdowncell>

# ### Querying object models with SPARQL using a triplestore
# 
# In this section we will show example SPARQL queries that access a triplestore.
# 
#     - What is a triplestore?
#     - How are NI-DM objects uploaded to a triplestore?
#     - How do we query a triplestore for data stored using NI-DM?

# <markdowncell>

# ### What is a triplestore?
# 
#     - A triplestore is a database for storing RDF triples (i.e., subject, predicate, object statements). 
#     - The triplestore provides a way to create, read, update, and delete persisted RDF statements using the SPARQL 1.1 protocol. 
#     - Here we are using the triplestore functionality of the Virtuoso database (http://virtuoso.openlinksw.com/) 
#     - The specific triplestore can easily be swapped for another system such as Jena or Seseme.

# <markdowncell>

# ### How are NIDM objects uploaded to a triplestore?
# 
#     - The examples above demonstrates how to create a collection of NI-DM FreeSurfer objects in RDF.
#     - RDF files can be bulk uploaded through the Virtuoso web interface or using a SPARQL Insert Statement
#     - This is an example of adding a single RDF triple

# <codecell>

import requests
from requests.auth import HTTPDigestAuth

from IPython.core.display import HTML


# connection params for secure endpoint
endpoint = 'http://computor.mit.edu:8890/sparql'
username = 'username'
password = 'password'

# session defaults
session = requests.Session()
session.auth = HTTPDigestAuth(username, password)

session.headers = {'Accept':'text/html'} # HTML from SELECT queries

# INSERT - CREATE new triples
query =  """PREFIX nidm: <http://nidm.nidash.org#>
            PREFIX prov: <http://www.w3.org/ns/prov#>
            PREFIX nlx: <http://uri.neuinfo.org/nif/nifstd/>
            
            INSERT DATA
            INTO GRAPH <http://nidm.nidash.org>
                {nidm:UUID12345 a prov:Entity;
                    nlx:nlx_150774 "2.0"}"""

data = {'query': query}
result = session.post(endpoint, data=data)
HTML(result.content)

# <markdowncell>

# ### How do we query a triplestore for data stored using NI-DM?
# 
#     - this example shows how to query for FreeSurfer details given a subject id

# <codecell>

    def getSubjectDetails(self, subject_id):
        session = requests.Session()

        qstring = '''
        PREFIX fs: <http://surfer.nmr.mgh.harvard.edu/fswiki/#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
        PREFIX nidm: <http://nidm.nidash.org/#>
        PREFIX prov: <http://www.w3.org/ns/prov#>

        select distinct ?structureName ?structureGV where {
             ?subjectCollection fs:subject_id "%s"^^<http://www.w3.org/2001/XMLSchema#string> .
             ?subjectCollection prov:hadMember ?otherMembers .
             ?collectionFromProv prov:wasDerivedFrom ?otherMembers .
             ?collectionFromProv prov:hadMember ?membersOfProvCollection .
             ?membersOfProvCollection a fs:GrayVol . # filter by those that have a type of fs:GrayVol(ume)
             ?membersOfProvCollection fs:structure ?structureName . 
             ?membersOfProvCollection fs:value ?structureGV .
        } ''' % (subject_id)

# <markdowncell>

# ### Mapping XCEDE primitives to NI-DM
# 
# The XCEDE XML schema allows for storing information in the context of a flexible and extensible experiment hierarchy. 
# 
# It accommodates arbitrary configurations centered around Project, Subject, Visit, Study, Episode, and Acquisition objects, as well as limited information about data provenance. Effectively defining a hierarchy of relationships.
# 
# It is ill-suited for modeling and querying across complex derived data created from many of today’s workflow systems.

# <markdowncell>

# ### Mappings
# 
# 
# - xcede:Project     -> prov:Activity (prov:type = nidm:Project)
# - xcede:Study       -> prov:Activity (prov:type = nidm:Study, dcterms:subpartOf = some_project)
# - xcede:Visit       -> prov:Activity (prov:type = nidm:Visit, dcterms:subpartOf = some_study)
# - xcede:Episode     -> prov:Activity (prov:type = nidm:Episod, dcterms:subpartOf = some_visit)
# - xcede:Acquisition -> prov:Activity (prov:type = nidm:Acquisition, dcterms:subpartOf = some_episode)

