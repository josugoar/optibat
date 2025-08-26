#!/usr/bin/env python

import heapq
import sys


def main():
    """
    Reads package names from command-line arguments and prints a valid,
    alphabetically-prioritized loading order that respects all predefined
    direct and transitive constraints.
    """

    packages_to_use = sys.argv[1:]

    all_constraints = [
        ("fancyhdr", "hyperref"),
        ("fncychap", "hyperref"),
        ("float", "hyperref"),
        ("hyperref", "algorithm"),
        ("amssymb", "xunicode"),
        ("amssymb", "xltxtra"),
        ("graphicx", "xltxtra"),
        ("graphicx", "fontspec"),
        ("caption", "subfig"),
        ("amsmath", "wasysym"),
        ("todonotes", "program"),
        ("algorithm2e", "cleveref"),
        ("algorithmicx", "cleveref"),
        ("aliascnt", "cleveref"),
        ("amsmath", "cleveref"),
        ("amsthm", "cleveref"),
        ("caption", "cleveref"),
        ("hyperref", "cleveref"),
        ("IEEEtrantools", "cleveref"),
        ("listings", "cleveref"),
        ("ntheorem", "cleveref"),
        ("subfig", "cleveref"),
        ("varioref", "cleveref"),
        ("cleveref", "autonum"),
        ("cleveref", "hypdvips"),
        ("varioref", "hyperref"),
        ("natbib", "citeref"),
        ("babel", "apacite"),
        ("hyperref", "apacite"),
        ("hyperref", "cmap"),
        ("hyperref", "ellipsis"),
        ("hyperref", "amsrefs"),
        ("hyperref", "chappg"),
        ("hyperref", "dblaccnt"),
        ("hyperref", "linguex"),
        ("multind", "hyperref"),
        ("natbib", "hyperref"),
        ("setspace", "hyperref"),
        ("hyperref", "glossaries"),
        ("babel", "glossaries"),
        ("polyglossia", "glossaries"),
        ("inputenc", "glossaries"),
        ("fontenc", "glossaries"),
        ("doc", "glossaries"),
        ("hyperref", "hypcap"),
        ("babel", "selnolig"),
        ("cmap", "fontenc"),
        ("mmap", "fontenc"),
        ("listings", "listingsutf8"),
        ("glossaries", "cleveref"),
        ("hyperref", "refenums"),
        ("cleveref", "refenums"),
        ("csquotes", "refenums"),
        ("ifthen", "refenums"),
        ("babel", "microtype"),
        ("hyperref", "uri"),
        ("url", "uri"),
        ("natbib", "hypernat"),
        ("hyperref", "hypernat"),
    ]

    all_packages_in_graph = set(p for constraint in all_constraints for p in constraint)
    full_graph = {pkg: set() for pkg in all_packages_in_graph}
    for before_pkg, after_pkg in all_constraints:
        full_graph[before_pkg].add(after_pkg)

    transitive_constraints = set()
    for start_node in full_graph:
        stack = list(full_graph[start_node])
        visited = set(full_graph[start_node])
        while stack:
            current_node = stack.pop()
            transitive_constraints.add((start_node, current_node))
            for neighbor in full_graph.get(current_node, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    stack.append(neighbor)
    final_constraints = set(all_constraints).union(transitive_constraints)

    adj = {pkg: [] for pkg in packages_to_use}
    in_degree = {pkg: 0 for pkg in packages_to_use}

    for before_pkg, after_pkg in final_constraints:
        if before_pkg in packages_to_use and after_pkg in packages_to_use:
            if after_pkg not in adj[before_pkg]:
                adj[before_pkg].append(after_pkg)
                in_degree[after_pkg] += 1

    ready_heap = [pkg for pkg in packages_to_use if in_degree[pkg] == 0]
    heapq.heapify(ready_heap)

    sorted_order = []
    while ready_heap:
        current_pkg = heapq.heappop(ready_heap)
        sorted_order.append(current_pkg)

        for dependent_pkg in sorted(adj[current_pkg]):
            in_degree[dependent_pkg] -= 1
            if in_degree[dependent_pkg] == 0:
                heapq.heappush(ready_heap, dependent_pkg)

    for package in sorted_order:
        print(package)


if __name__ == "__main__":
    main()
