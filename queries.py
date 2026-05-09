"""
Benchmark queries for the LiveJournal graph.
Graph: 3,997,962 nodes, 69,362,378 directed edges (symmetrized), no properties.

All queries use forward-only traversal and return a single aggregate row —
multi-row streaming has a separate engine bug that affects both backends equally.
"""

# Selected node IDs
HIGH_DEG_NODE = 9766    # out-degree 14,815
MED_DEG_NODE  = 3       # out-degree 454
LOW_DEG_NODE  = 1000    # out-degree ~10 (sparse)
NODE_A        = 9765    # another high-degree node

QUERIES = [
    {
        "id": "q01_count_nodes",
        "description": "Count all nodes",
        "cypher": "MATCH (u:user) RETURN count(*)",
    },
    {
        "id": "q02_count_edges_meta",
        "description": "Count all edges (metadata fast path)",
        "cypher": "MATCH ()-[:follows]->() RETURN count(*)",
    },
    {
        "id": "q03_outdeg_high",
        "description": f"Out-degree of high-degree node ({HIGH_DEG_NODE}, deg=14815)",
        "cypher": f"MATCH (u:user {{id: {HIGH_DEG_NODE}}})-[:follows]->(v) RETURN count(v)",
    },
    {
        "id": "q04_outdeg_med",
        "description": f"Out-degree of medium-degree node ({MED_DEG_NODE}, deg=454)",
        "cypher": f"MATCH (u:user {{id: {MED_DEG_NODE}}})-[:follows]->(v) RETURN count(v)",
    },
    {
        "id": "q05_outdeg_low",
        "description": f"Out-degree of low-degree node ({LOW_DEG_NODE})",
        "cypher": f"MATCH (u:user {{id: {LOW_DEG_NODE}}})-[:follows]->(v) RETURN count(v)",
    },
    {
        "id": "q06_top10_outdeg",
        "description": "Top-10 nodes by out-degree",
        "cypher": (
            "MATCH (u:user)-[:follows]->(v) "
            "RETURN u.id, count(v) AS deg ORDER BY deg DESC LIMIT 10"
        ),
    },
    {
        "id": "q07_count_active_src",
        "description": "Count nodes with at least one out-edge",
        "cypher": (
            "MATCH (u:user)-[:follows]->(v) "
            "RETURN count(DISTINCT u.id)"
        ),
    },
    {
        "id": "q08_full_scan_rel",
        "description": "Full edge scan — count bound rel variable",
        "cypher": "MATCH (a:user)-[f:follows]->(b:user) RETURN count(f)",
    },
    {

        "id": "q09_full_scan_star",
        "description": "Full edge scan — count(*) (no rel var)",
        "cypher": "MATCH (a:user)-[:follows]->(b:user) RETURN count(*)",
    },
    {
        "id": "q10_outdeg_node_a",
        "description": f"Out-degree of second high-degree node ({NODE_A}, deg≈12870)",
        "cypher": f"MATCH (u:user {{id: {NODE_A}}})-[:follows]->(v) RETURN count(v)",
    },
]
