-- GET ALL NODES AND EDGES 
MATCH (p:uri) RETURN p; 

-- DELETE ALL NODES AND EDGES 
MATCH (n)
OPTIONAL MATCH (n)-[r]-()
DELETE n,r

-- CREATE NODES AND EDGES

CREATE 
(galen_Areflexia:uri { name : 'GALEN: Areflexia'}),
(OMIM_MTHU002841:uri { name : 'OMIM: Absent ankle reflexes'}),
(OMIM_MTHU016966:uri { name : 'OMIM: Absent ankle jerks'}),
(OMIM_MTHU016567:uri { name : 'OMIM: Decreased gag reflex'}),
(OMIM_145290:uri { name : 'OMIM: HYPERREFLEXIA'}),
(OMIM_MTHU000140:uri { name : 'OMIM: Hyperreflexia'}),
(OMIM_MTHU005701:uri { name : 'OMIM: Absent corneal reflexes'}),
(OMIM_MTHU013353:uri { name : 'OMIM: Decreased corneal reflex'}),
(galen_Hyperreflexia:uri { name : 'GALEN: Hyperreflexia'}),
(OMIM_MTHU012678:uri { name : 'OMIM: Absent deep tendon reflexes'}),
(OMIM_MTHU038604:uri { name : 'OMIM: Increased tendon reflex'}),
(OMIM_MTHU033523:uri { name : 'OMIM: Abnormal reflexes'}),
(OMIM_167700:uri { name : 'OMIM: PALMOMENTAL REFLEX'}),
(OMIM_MTHU033901:uri { name : 'OMIM: Hyperrflexia'}),
(OMIM_MTHU000328:uri { name : 'OMIM: Hyporeflexia'}),
(C43248:uri { name : 'NCIT: Hyperreflexia'}),
(OMIM_MTHU012530:uri { name : 'OMIM: Increased deep tendon reflexes'}),
(MESH_D012021:uri { name : 'MESH: Reflex, Abnormal'}),
(OMIM_MTHU000329:uri { name : 'OMIM: Areflexia'}),
(C115420:uri { name : 'NCIT: Areflexia'}),
(OMIM_MTHU010731:uri { name : 'OMIM: Absent gag reflex'}),
(galen_Areflexia)-[:EQUALS]->(OMIM_MTHU000329),
(galen_Hyperreflexia)-[:EQUALS]->(OMIM_MTHU000140),
(galen_Hyperreflexia)-[:EQUALS]->(OMIM_145290),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU038604),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU000140),
(MESH_D012021)-[:EQUALS]->(OMIM_167700),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU000328),
(MESH_D012021)-[:EQUALS]->(OMIM_145290),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU033523),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU013353),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU033901),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU012530),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU012678),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU016567),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU005701),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU000329),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU002841),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU010731),
(MESH_D012021)-[:EQUALS]->(OMIM_MTHU016966),
(C43248)-[:EQUALS]->(galen_Hyperreflexia),
(C115420)-[:EQUALS]->(galen_Areflexia),
(C43248)-[:EQUALS]->(OMIM_MTHU000140),
(C43248)-[:EQUALS]->(OMIM_145290),
(C115420)-[:EQUALS]->(OMIM_MTHU000329),