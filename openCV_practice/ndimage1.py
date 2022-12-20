import networkx as nx
import numpy as np
from scipy import ndimage as ndi
from scipy.ndimage.filters import generic_filter

def add_edge_filter ( values, graph ):
    center = values[len(values)//2]
    for neighbor in values:
        if neighbor != center and not graph.has_edge(center, neighbor):
            graph.add_edge(center, neighbor)

    return 0.0

def build_rag(labels, image):
    g = nx.Graph()
    footprint = ndi.generate_binary_structure(labels.ndim, connectivity=1)
    _ = ndi.generic_filter(labels, add_edge_filter, footprint=footprint, mode='nearest', extra_arguments=(g,))
    return g


### 이게 뭔가요?     