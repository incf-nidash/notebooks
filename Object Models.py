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
from nipype.utils.filemanip import hash_infile

# create namespace references to terms used
foaf = prov.Namespace("foaf", "http://xmlns.com/foaf/0.1/")
dcterms = prov.Namespace("dcterms", "http://purl.org/dc/terms/")
fs = prov.Namespace("fs", "http://surfer.nmr.mgh.harvard.edu/fswiki/terms/0.1/")
nidm = prov.Namespace("nidm", "http://nidm.nidash.org/terms/0.1/")

# <markdowncell>

# ### Define a function that creates a NI-DM Entity

# <codecell>

def create_entity(graph, fullpath, root, subject_id, basedir):
    """ Create a PROV entity for a file in a FreeSurfer directory
    """
    # identify FreeSurfer terms based on directory and file names
    relpath = fullpath.replace(basedir, '').replace(root, '').lstrip('/')
    fstypes = relpath.split('/')[:-1]
    additional_types = relpath.split('/')[-1].split('.')
    
    file_hash = hash_infile(fullpath)
    if file_hash is None:
        print fullpath

    # build a NI-DM object 
    obj_attr = [(prov.PROV["type"], fs[fstype]) for fstype in fstypes] + \
               [(prov.PROV["label"], "%s:%s" % ('.'.join(fstypes), '.'.join(additional_types))),
                (fs["relative_path"], "%s" % relpath),
                (nidm["file"], "file://%s%s" % (getfqdn(), fullpath)),
                (nidm["md5sum"], "%s" % file_hash),
               ]
    # append approprate FreeSurfer terms
    if 'lh' in additional_types:
        obj_attr.append((fs["hemisphere"], "left"))
    if 'rh' in additional_types:
        obj_attr.append((fs["hemisphere"], "right"))
    if 'aparc' in additional_types:
        obj_attr.append((prov.PROV["type"], fs["aparc"]))
    if 'a2005s' in additional_types:
        obj_attr.append((prov.PROV["type"], fs["a2005s"]))                
    if 'a2009s' in additional_types:
        obj_attr.append((prov.PROV["type"], fs["a2009s"]))                
    if 'exvivo' in relpath:
        obj_attr.append((prov.PROV["type"], fs["exvivo"]))                
    if 'aseg' in additional_types:
        obj_attr.append((prov.PROV["type"], fs["aseg"]))                
    if 'aparc.stats' in relpath:
        obj_attr.append((prov.PROV["type"], fs["desikan_killiany"]))                
    if 'stats' in fstypes and 'stats' in additional_types:
        obj_attr.append((prov.PROV["type"], fs["statistics"]))
    id = fs[md5.new(subject_id + relpath).hexdigest()]
    graph.entity(id, obj_attr)
    return id

# <markdowncell>

# ### Terms introduced
# 
#     - relative_path
#     - file
#     - md5sum
#     - fs: mri, label, stats, scripts, touch  - from directory names 
#     - aparc, a2005s, a2009s, exvivo, aseg, desikan_killiany
#     - statistics

# <markdowncell>

# ### Define a function that manages creating a NI-DM FreeSurfer object

# <codecell>

def freesurfer2provgraph(g, dirname, basedir, n_items=100000):
    """ Convert a FreeSurfer directory to a PROV graph
    """
    subject_id = dirname.rstrip('/').split('/')[-1]
    project_string = '-'.join(dirname.replace(basedir, '').rstrip('/').split('/')[1:-1])
    # directory collection/catalog
    collection_hash = md5.new(project_string + ':' + subject_id).hexdigest()
    fsdir_collection = g.collection(fs[collection_hash])
    fsdir_collection.add_extra_attributes({prov.PROV['type']: fs['directory'],
                                           fs['subject_id']: subject_id,
                                           nidm['annotation']: project_string})
    i = 0;
    for dirpath, dirnames, filenames in os.walk(os.path.realpath(os.path.join(basedir, dirname))):
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
            try:
                id = create_entity(g, file2encode, dirname[2:], subject_id, basedir)
                g.hadMember(fsdir_collection, id)
            except IOError, e:
                print e
    return g

# <markdowncell>

# ### Example of generating a NI-DM FreeSurfer Object
# 
# This example demonstrates the conversion from [PROV Notation](http://www.w3.org/TR/prov-n/) to RDF using the [ProvToolBox](https://github.com/lucmoreau/ProvToolbox), which provides a command line utility for serializing PROV to different syntaxes.

# <codecell>

# location of FreeSurfer $SUBJECTS_DIR
basedir = '/mindhive/xnat/surfaces/'

from subprocess import Popen, PIPE

# location of the ProvToolBox commandline conversion utility
pc = '/mindhive/gablab/satra/ProvToolbox/toolbox/target/appassembler/bin/provconvert'
for dirname in dirnames:
    name = '-'.join(dirname.rstrip('/').split('/')[1:])
    filename = '../store/%s.provn' % name
    if not os.path.exists('%s.ttl' % filename):
        graph = prov.ProvBundle()
        graph.add_namespace(foaf)
        graph.add_namespace(dcterms)
        graph.add_namespace(fs)
        graph.add_namespace(nidm)
        freesurfer2provgraph(graph, dirname)
        print dirname
        with open(filename, 'wt') as fp:
            fp.writelines(graph.get_provn())
        o, e = Popen('%s -infile %s -outfile %s.ttl' % (pc, filename, filename), stdout=PIPE, 
                     stderr=PIPE, shell=True).communicate()
        print('converted %s' % filename)
    else:
        print "%s exists" % filename

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

