"""
Benchmark queries for the LiveJournal graph.
Graph: 3,997,962 nodes, 69,362,378 directed edges (symmetrized), no properties.
"""

# Selected node IDs
HIGH_DEG_NODE = 9766    # out-degree 14,815
MED_DEG_NODE  = 3       # out-degree 454
LOW_DEG_NODE  = 1000    # out-degree ~10 (sparse)
NODE_A        = 9765    # another high-degree node

# Threshold for "hub" nodes used in q19
HUB_OUTDEG_THRESHOLD = 100

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

    # ------------------------------------------------------------------ #
    # In-degree / follower queries                                         #
    # ------------------------------------------------------------------ #
    {
        "id": "q11_indeg_high",
        "description": f"In-degree (follower count) of high-degree node ({HIGH_DEG_NODE})",
        "cypher": f"MATCH (v)-[:follows]->(u:user {{id: {HIGH_DEG_NODE}}}) RETURN count(v)",
    },
    {
        "id": "q12_top10_indeg",
        "description": "Top-10 most-followed nodes (influencer detection)",
        "cypher": (
            "MATCH (v)-[:follows]->(u:user) "
            "RETURN u.id, count(v) AS indeg ORDER BY indeg DESC LIMIT 10"
        ),
    },

    # ------------------------------------------------------------------ #
    # Reciprocal / mutual relationships                                    #
    # Real-world: "bidirectional friendship" detection used in            #
    # Facebook's EdgeRank and Twitter's follow analytics.                 #
    # ------------------------------------------------------------------ #
    {
        "id": "q13_mutual_follows_count",
        "description": "Count reciprocal (mutual) follow pairs across the full graph",
        "cypher": (
            "MATCH (a:user)-[:follows]->(b:user)-[:follows]->(a) "
            "WHERE a.id < b.id "
            "RETURN count(*) AS mutual_pairs"
        ),
    },
    {
        "id": "q14_mutual_follows_of_node",
        "description": f"List nodes in mutual-follow relationship with {HIGH_DEG_NODE}",
        "cypher": (
            f"MATCH (u:user {{id: {HIGH_DEG_NODE}}})-[:follows]->(v)-[:follows]->(u) "
            "RETURN count(v) AS mutual_count"
        ),
    },

    # ------------------------------------------------------------------ #
    # Friend recommendation — "People You May Know"                        #
    # LinkedIn/Facebook workload: rank 2-hop neighbors by overlap score.  #
    # Source: LDBC SNB Interactive IC2-IC3 patterns; real PYMK workloads. #
    # ------------------------------------------------------------------ #
    {
        "id": "q15_pymk_high",
        "description": f"PYMK recommendations for {HIGH_DEG_NODE}: top-10 2-hop nodes scored by path count",
        "cypher": (
            f"MATCH (u:user {{id: {HIGH_DEG_NODE}}})-[:follows]->()-[:follows]->(rec) "
            f"WHERE NOT (u)-[:follows]->(rec) AND rec.id <> {HIGH_DEG_NODE} "
            "RETURN rec.id, count(*) AS score ORDER BY score DESC LIMIT 10"
        ),
    },
    {
        "id": "q16_pymk_med",
        "description": f"PYMK recommendations for {MED_DEG_NODE}: top-10 2-hop nodes scored by path count",
        "cypher": (
            f"MATCH (u:user {{id: {MED_DEG_NODE}}})-[:follows]->()-[:follows]->(rec) "
            f"WHERE NOT (u)-[:follows]->(rec) AND rec.id <> {MED_DEG_NODE} "
            "RETURN rec.id, count(*) AS score ORDER BY score DESC LIMIT 10"
        ),
    },

    # ------------------------------------------------------------------ #
    # Common follows / mutual friends                                      #
    # Jaccard similarity numerator; used for link prediction and          #
    # community detection (Adamic-Adar, Jaccard — Liben-Nowell 2003).    #
    # ------------------------------------------------------------------ #
    {
        "id": "q17_common_follows",
        "description": f"Common follows between {HIGH_DEG_NODE} and {NODE_A} (mutual friends count)",
        "cypher": (
            f"MATCH (a:user {{id: {HIGH_DEG_NODE}}})-[:follows]->(c)<-[:follows]-(b:user {{id: {NODE_A}}}) "
            "RETURN count(DISTINCT c) AS common_follows"
        ),
    },

    # ------------------------------------------------------------------ #
    # Ego network                                                          #
    # 1-hop undirected neighbourhood — used in ego-network analysis       #
    # (McAuley & Leskovec 2012, "Learning to Discover Social Circles").   #
    # ------------------------------------------------------------------ #
    {
        "id": "q18_ego_net_size",
        "description": f"Ego network size of {HIGH_DEG_NODE} (1-hop, undirected)",
        "cypher": (
            f"MATCH (u:user {{id: {HIGH_DEG_NODE}}})-[:follows]-(v) "
            "RETURN count(DISTINCT v) AS ego_size"
        ),
    },

    # ------------------------------------------------------------------ #
    # BFS / reachability                                                   #
    # Core workload of Graph500 and LDBC SNB IC13 (SSSP).                #
    # ------------------------------------------------------------------ #
    {
        "id": "q19_2hop_reach",
        "description": f"Distinct nodes reachable within 2 hops from {HIGH_DEG_NODE} (BFS depth-2)",
        "cypher": (
            f"MATCH (u:user {{id: {HIGH_DEG_NODE}}})-[:follows*1..2]->(v) "
            "RETURN count(DISTINCT v)"
        ),
    },
    {
        "id": "q20_3hop_reach",
        "description": f"Distinct nodes reachable within 3 hops from {MED_DEG_NODE} (BFS depth-3)",
        "cypher": (
            f"MATCH (u:user {{id: {MED_DEG_NODE}}})-[:follows*1..3]->(v) "
            "RETURN count(DISTINCT v)"
        ),
    },

    # ------------------------------------------------------------------ #
    # Shortest path (LDBC SNB IC13)                                        #
    # "6 degrees of separation" — canonical social graph query.           #
    # ------------------------------------------------------------------ #
    {
        "id": "q21_shortest_path",
        "description": f"Shortest path length between {HIGH_DEG_NODE} and {LOW_DEG_NODE}",
        "cypher": (
            f"MATCH (a:user {{id: {HIGH_DEG_NODE}}})-[e:follows* SHORTEST 1..30]->(b:user {{id: {LOW_DEG_NODE}}}) "
            "RETURN length(e) AS path_length"
        ),
    },
    {
        "id": "q22_all_shortest_paths",
        "description": f"Count of all shortest paths between {HIGH_DEG_NODE} and {NODE_A}",
        "cypher": (
            f"MATCH (a:user {{id: {HIGH_DEG_NODE}}})-[e:follows* ALL SHORTEST 1..30]-(b:user {{id: {NODE_A}}}) "
            "RETURN count(*) AS num_shortest_paths, min(length(e)) AS path_length"
        ),
    },

    # ------------------------------------------------------------------ #
    # Triangle counting                                                    #
    # Fundamental for community detection (Leskovec et al. 2009),        #
    # clustering coefficient, and dense subgraph discovery.              #
    # ------------------------------------------------------------------ #
    {
        "id": "q23_triangles_node",
        "description": f"Triangle count through {MED_DEG_NODE} (local)",
        "cypher": (
            f"MATCH (u:user {{id: {MED_DEG_NODE}}})-[:follows]->(v)-[:follows]->(w)-[:follows]->(u) "
            "RETURN count(*) AS triangles"
        ),
    },
    {
        "id": "q24_global_triangles",
        "description": "Global triangle count (all directed 3-cycles, canonical ordering)",
        "cypher": (
            "MATCH (a:user)-[:follows]->(b:user)-[:follows]->(c:user)-[:follows]->(a) "
            "WHERE a.id < b.id AND b.id < c.id "
            "RETURN count(*) AS triangles"
        ),
    },

    # ------------------------------------------------------------------ #
    # Local clustering coefficient                                         #
    # Ratio of edges among a node's neighbours to the maximum possible.  #
    # Watts & Strogatz 1998; standard social graph metric.               #
    # ------------------------------------------------------------------ #
    {
        "id": "q25_clustering_coeff",
        "description": f"Directed local clustering coefficient of {MED_DEG_NODE}",
        "cypher": (
            f"MATCH (u:user {{id: {MED_DEG_NODE}}})-[:follows]->(v) "
            "WITH u, count(v) AS deg "
            "MATCH (u)-[:follows]->(n1)-[:follows]->(n2)<-[:follows]-(u) "
            "WHERE n1 <> n2 "
            "WITH u, deg, count(*) AS inner_edges "
            "RETURN u.id, deg, inner_edges, "
            "inner_edges * 1.0 / (deg * (deg - 1)) AS directed_cc"
        ),
    },

    # ------------------------------------------------------------------ #
    # Network structure: sinks and sources                                 #
    # Identifies dangling nodes — important for PageRank convergence      #
    # and graph diameter estimation.                                       #
    # ------------------------------------------------------------------ #
    {
        "id": "q26_sink_nodes",
        "description": "Count sink nodes (no outgoing follows — pure consumers)",
        "cypher": (
            "MATCH (u:user) WHERE NOT (u)-[:follows]->() RETURN count(u) AS sinks"
        ),
    },
    {
        "id": "q27_source_nodes",
        "description": "Count source nodes (no incoming follows — pure broadcasters)",
        "cypher": (
            "MATCH (u:user) WHERE NOT ()-[:follows]->(u) RETURN count(u) AS sources"
        ),
    },

    # ------------------------------------------------------------------ #
    # Hub detection                                                        #
    # Nodes with both high in-degree and out-degree — "connector" hubs.  #
    # Used in influence propagation and network resilience studies.       #
    # ------------------------------------------------------------------ #
    {
        "id": "q28_hub_nodes",
        "description": f"Count nodes with out-degree >= {HUB_OUTDEG_THRESHOLD} AND in-degree >= {HUB_OUTDEG_THRESHOLD}",
        "cypher": (
            "MATCH (u:user)-[:follows]->(v) "
            f"WITH u, count(v) AS outdeg WHERE outdeg >= {HUB_OUTDEG_THRESHOLD} "
            "MATCH ()-[:follows]->(u) "
            f"WITH u, outdeg, count(*) AS indeg WHERE indeg >= {HUB_OUTDEG_THRESHOLD} "
            "RETURN count(u) AS hubs"
        ),
    },

    # ------------------------------------------------------------------ #
    # Dense subgraph / k-core proxy                                        #
    # k-core decomposition is iterative; this counts nodes passing a     #
    # single degree threshold — a common approximation used in            #
    # Batagelj & Zaversnik 2003 and follow-on LiveJournal studies.       #
    # ------------------------------------------------------------------ #
    {
        "id": "q29_high_outdeg_nodes",
        "description": "Count nodes with out-degree >= 50 (dense-subgraph / k-core proxy)",
        "cypher": (
            "MATCH (u:user)-[:follows]->(v) "
            "WITH u, count(v) AS deg WHERE deg >= 50 "
            "RETURN count(u)"
        ),
    },

    # ------------------------------------------------------------------ #
    # 2-hop exclusion (true FoF not already followed)                     #
    # Exact formulation used in LinkedIn's PYMK pipeline and in          #
    # "The LinkedIn Economic Graph" (Shah et al. 2015).                  #
    # ------------------------------------------------------------------ #
    {
        "id": "q30_fof_not_followed",
        "description": f"Count distinct FoF of {HIGH_DEG_NODE} not already followed (exact PYMK candidate set)",
        "cypher": (
            f"MATCH (u:user {{id: {HIGH_DEG_NODE}}})-[:follows]->()-[:follows]->(w) "
            f"WHERE NOT (u)-[:follows]->(w) AND w.id <> {HIGH_DEG_NODE} "
            "RETURN count(DISTINCT w)"
        ),
    },

    # ------------------------------------------------------------------ #
    # Jaccard similarity between two nodes' neighbourhoods                #
    # |N(A) ∩ N(B)| / |N(A) ∪ N(B)| — canonical link-prediction        #
    # feature (Liben-Nowell & Kleinberg 2003, CIKM; widely used in      #
    # recommendation and duplicate detection pipelines).                  #
    # ------------------------------------------------------------------ #
    {
        "id": "q31_jaccard_similarity",
        "description": f"Jaccard similarity of out-neighbourhoods of {HIGH_DEG_NODE} and {NODE_A}",
        "cypher": (
            f"MATCH (a:user {{id: {HIGH_DEG_NODE}}})-[:follows]->(c)<-[:follows]-(b:user {{id: {NODE_A}}}) "
            "WITH count(DISTINCT c) AS inter "
            f"MATCH (a:user {{id: {HIGH_DEG_NODE}}})-[:follows]->(na) "
            "WITH inter, count(DISTINCT na) AS size_a "
            f"MATCH (b:user {{id: {NODE_A}}})-[:follows]->(nb) "
            "WITH inter, size_a, count(DISTINCT nb) AS size_b "
            "RETURN inter * 1.0 / (size_a + size_b - inter) AS jaccard"
        ),
    },

    # ------------------------------------------------------------------ #
    # Adamic-Adar score                                                    #
    # Σ 1/log(|N(c)|) over common neighbours c — outperforms Jaccard    #
    # on LiveJournal in the original Liben-Nowell 2003 paper.            #
    # ------------------------------------------------------------------ #
    {
        "id": "q32_adamic_adar",
        "description": f"Adamic-Adar link-prediction score between {HIGH_DEG_NODE} and {NODE_A}",
        "cypher": (
            f"MATCH (a:user {{id: {HIGH_DEG_NODE}}})-[:follows]->(c)<-[:follows]-(b:user {{id: {NODE_A}}}) "
            "MATCH (x)-[:follows]->(c) "
            "WITH c, count(x) AS indeg "
            "RETURN sum(1.0 / log(indeg)) AS adamic_adar_score"
        ),
    },

    # ------------------------------------------------------------------ #
    # Structural degree queries                                            #
    # Nodes with exactly 1 follower / 1 followee — leaf detection used   #
    # in k-core pruning and graph sparsification (Batagelj & Zaversnik). #
    # ------------------------------------------------------------------ #
    {
        "id": "q33_indeg_exactly_1",
        "description": "Count nodes with exactly 1 follower (in-degree = 1)",
        "cypher": (
            "MATCH (v)-[:follows]->(u:user) "
            "WITH u, count(v) AS indeg WHERE indeg = 1 "
            "RETURN count(u) AS nodes_with_indeg_1"
        ),
    },
    {
        "id": "q34_outdeg_exactly_1",
        "description": "Count nodes with exactly 1 outgoing follow (out-degree = 1)",
        "cypher": (
            "MATCH (u:user)-[:follows]->(v) "
            "WITH u, count(v) AS outdeg WHERE outdeg = 1 "
            "RETURN count(u) AS nodes_with_outdeg_1"
        ),
    },

    # ------------------------------------------------------------------ #
    # Reverse BFS reachability                                             #
    # Who can reach a given node following directed edges?                #
    # Used in influence estimation and cascade modelling                  #
    # (Kempe, Kleinberg & Tardos 2003, KDD).                             #
    # ------------------------------------------------------------------ #
    {
        "id": "q35_reverse_2hop_reach",
        "description": f"Distinct nodes that can reach {HIGH_DEG_NODE} within 2 hops (reverse BFS)",
        "cypher": (
            f"MATCH (v)-[:follows*1..2]->(u:user {{id: {HIGH_DEG_NODE}}}) "
            "RETURN count(DISTINCT v) AS reverse_2hop_reach"
        ),
    },

    # ------------------------------------------------------------------ #
    # In-degree histogram bucket                                           #
    # Distribution shape used in degree-distribution power-law analysis  #
    # (Barabási & Albert 1999; Leskovec et al. SIGKDD 2005).             #
    # ------------------------------------------------------------------ #
    {
        "id": "q36_indeg_distribution",
        "description": "In-degree distribution: count of nodes per log-10 bucket",
        "cypher": (
            "MATCH (v)-[:follows]->(u:user) "
            "WITH u, count(v) AS indeg "
            "RETURN floor(log10(indeg)) AS log10_bucket, count(u) AS node_count "
            "ORDER BY log10_bucket"
        ),
    },

    # ================================================================== #
    # Graph algorithm queries — Ladybug `algo` extension                  #
    # Require: INSTALL algo; LOAD algo; CALL project_graph('lj', ...)    #
    # (added to schema.cypher for both backends)                          #
    # ================================================================== #

    # ------------------------------------------------------------------ #
    # PageRank                                                             #
    # Ranks nodes by link-based authority. Standard benchmark kernel in   #
    # Ligra, Gunrock, GraphBLAS/LAGraph, and LDBC SNB BI workloads.      #
    # Ladybug uses a parallelised Ligra-based implementation.             #
    # ------------------------------------------------------------------ #
    {
        "id": "q37_pagerank_top10",
        "description": "PageRank — top-10 most influential users (damping=0.85, iter=20)",
        "cypher": (
            "CALL page_rank('lj') "
            "RETURN node.id AS id, rank "
            "ORDER BY rank DESC LIMIT 10"
        ),
    },

    # ------------------------------------------------------------------ #
    # Weakly Connected Components (WCC)                                   #
    # Finds clusters in the undirected projection. LiveJournal's giant   #
    # WCC contains ~99.9% of all nodes. Benchmarked in Ligra, Gunrock,  #
    # GraphBLAS LAGraph, and virtually all distributed graph systems.    #
    # ------------------------------------------------------------------ #
    {
        "id": "q38_wcc_sizes",
        "description": "WCC — top-10 weakly-connected components by size",
        "cypher": (
            "CALL wcc('lj') "
            "RETURN group_id, count(*) AS size "
            "ORDER BY size DESC LIMIT 10"
        ),
    },
    {
        "id": "q39_wcc_component_count",
        "description": "WCC — total number of weakly-connected components",
        "cypher": (
            "CALL wcc('lj') "
            "RETURN count(DISTINCT group_id) AS num_components"
        ),
    },

    # ------------------------------------------------------------------ #
    # Strongly Connected Components (SCC)                                 #
    # LiveJournal's giant SCC contains ~79% of nodes (3.83M).           #
    # Leskovec et al. Internet Mathematics 2009; SNAP dataset page.      #
    # Two algorithms: parallel BFS coloring and single-threaded Kosaraju.#
    # ------------------------------------------------------------------ #
    {
        "id": "q40_scc_sizes",
        "description": "SCC (parallel BFS) — top-10 strongly-connected components by size",
        "cypher": (
            "CALL scc('lj') "
            "RETURN group_id, count(*) AS size "
            "ORDER BY size DESC LIMIT 10"
        ),
    },
    {
        "id": "q41_scc_kosaraju_sizes",
        "description": "SCC (Kosaraju DFS) — top-10 strongly-connected components by size",
        "cypher": (
            "CALL scc_ko('lj') "
            "RETURN group_id, count(*) AS size "
            "ORDER BY size DESC LIMIT 10"
        ),
    },

    # ------------------------------------------------------------------ #
    # Louvain community detection                                          #
    # Maximises modularity on the undirected projection.                 #
    # LiveJournal has ground-truth community annotations used in         #
    # Leskovec et al. 2009 and BigCLAM (Yang & Leskovec, WSDM 2013).    #
    # ------------------------------------------------------------------ #
    {
        "id": "q42_louvain_top10",
        "description": "Louvain — top-10 communities by size (modularity optimisation)",
        "cypher": (
            "CALL louvain('lj') "
            "RETURN louvain_id, count(*) AS size "
            "ORDER BY size DESC LIMIT 10"
        ),
    },
    {
        "id": "q43_louvain_count",
        "description": "Louvain — total number of detected communities",
        "cypher": (
            "CALL louvain('lj') "
            "RETURN count(DISTINCT louvain_id) AS num_communities"
        ),
    },

    # ------------------------------------------------------------------ #
    # K-Core Decomposition                                                 #
    # Finds the maximal subgraph where every node has degree >= k.       #
    # Used for dense subgraph discovery and network resilience analysis.  #
    # Batagelj & Zaversnik 2003; Ligra k-core benchmark kernel.         #
    # ------------------------------------------------------------------ #
    {
        "id": "q44_kcore_top10",
        "description": "K-Core decomposition — top-10 nodes by core number",
        "cypher": (
            "CALL kcore('lj') "
            "RETURN node.id AS id, k_degree "
            "ORDER BY k_degree DESC LIMIT 10"
        ),
    },
    {
        "id": "q45_kcore_distribution",
        "description": "K-Core decomposition — node count per core number (distribution)",
        "cypher": (
            "CALL kcore('lj') "
            "RETURN k_degree, count(*) AS node_count "
            "ORDER BY k_degree DESC"
        ),
    },
]
