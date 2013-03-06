# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <markdowncell>

# # NI-DM: A Summary in many parts
# 
# ## Part 1: Constructing and Querying Object Models
# 
# satra@mit.edu, ...
# 
# [Latest version](https://github.com/INCF/ni-dm/tree/source/source/notebooks/Object models.ipynb)

# <markdowncell>

# ## Outline
# 
# - What is a data model, provenance, PROV-DM and NI-DM?
# - What is an object model?
# - Using the PROV API to construct object models
# - Querying object models from triple stores with SPARQL
# - Mapping XCEDE primitives to NI-DM

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
# [PROV-DM](http://www.w3.org/TR/prov-dm/) is the conceptual data model that forms a basis for the W3C provenance (PROV) family of specifications.
# 
# PROV-DM is organized in six components, respectively dealing with: (1) entities and activities, and the time at which they were created, used, or ended; (2) derivations of entities from entities; (3) agents bearing responsibility for entities that were generated and activities that happened; (4) a notion of bundle, a mechanism to support provenance of provenance; (5) properties to link entities that refer to the same thing; and, (6) collections forming a logical structure for its members.
# 
# ## What is NI-DM?
# 
# NI-DM is formulated as a domain specific extension of PROV-DM, but at this point maps identically to PROV-DM and domain extensions are captured as  object models on top of PROV-DM.

# <markdowncell>

# ### Basic Provenance data model
# 
# <img src="http://www.w3.org/TR/prov-o/diagrams/starting-points.svg" />

# <markdowncell>

# ## What is an object model?
# 
# An object model represents a collection "through which a program can examine and manipulate some specific parts of its world."
# 
# ## What are object models in NI-DM?
# 
# In the context of NI-DM, object models capture specific relationships between [entities][entity] via [collections][collection] that reflect organization information derived from imaging files (e.g., DICOM, Nifti, MINC), directory structures (e.g., Freesurfer, OpenFMRI), phenotypic data (e.g., neuropsych assessments, csv files) and binary or text files (e.g., SPM.mat, Feat.fsf, aseg.stats).
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
# In particular we have generated many terms in the FreeSurfer namespace [http://surfer.nmr.mgh.harvard.edu/fswiki/terms/0.1/]. This namespace should be managed by FreeSurfer developers and should provide an RDF representation of the terms in this namespace.
# 
# Some example terms are:
#     
#     - subject_id : participant id
#     - relative_path : location of file from the root of the subject directory structure

# <markdowncell>

# #### Import necessary tools and create namespaces

# <codecell>

import os
import md5
from socket import getfqdn

import prov.model as prov
from nipype.utils.filemanip import hash_infile

foaf = prov.Namespace("foaf", "http://xmlns.com/foaf/0.1/")
dcterms = prov.Namespace("dcterms", "http://purl.org/dc/terms/")
fs = prov.Namespace("fs", "http://surfer.nmr.mgh.harvard.edu/fswiki/terms/0.1/")
nidm = prov.Namespace("nidm", "http://nidm.nidash.org/terms/0.1/")

# <codecell>

def create_entity(graph, fullpath, root, subject_id, basedir):
    """ Create a PROV entity for a file in a FreeSurfer directory
    """
    relpath = fullpath.replace(basedir, '').replace(root, '').lstrip('/')
    fstypes = relpath.split('/')[:-1]
    additional_types = relpath.split('/')[-1].split('.')
    
    file_hash = hash_infile(fullpath)
    if file_hash is None:
        print fullpath
    obj_attr = [(prov.PROV["type"], fs[fstype]) for fstype in fstypes] + \
               [(prov.PROV["label"], "%s:%s" % ('.'.join(fstypes), '.'.join(additional_types))),
                (fs["relative_path"], "%s" % relpath),
                (nidm["file"], "file://%s%s" % (getfqdn(), fullpath)),
                (nidm["md5sum"], "%s" % file_hash),
               ]
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

# <codecell>

basedir = '/mindhive/xnat/surfaces/'
from subprocess import Popen, PIPE
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

