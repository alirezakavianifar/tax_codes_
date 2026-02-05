from heapq import heappush, heappop
import numpy as np


class Node:

    def __init__(self, name, position, parent=None):
        self.name = name
        self.position = position
        self.parent = parent
        self.neighbors = []
        self.g = 0
        self.h = 0
        self.f = 0

    def add_neighbor(self, v):
        self.neighbors.append(v)

    def __repr__(self):
        return self.name

    def __lt__(self, other_node):
        return self.f < other_node.f


class Edge:

    def __init__(self, target, weight):
        self.target = target
        self.weight = weight


class SearchAlgorithm:
    def __init__(self, source, destination):
        self.source = source
        self.destination = destination
        self.explored = set()
        self.heap = [source]

    def run(self):

        while self.heap:
            current = heappop(self.heap)
            self.explored.add(current)

            if current == self.destination:
                break

            for edge in current.neighbors:
                child = edge.target
