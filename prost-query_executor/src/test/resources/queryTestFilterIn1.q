SELECT ?name
WHERE 
{
  ?x <http://example.org/name> ?name .
  FILTER IN{"C", "D"}
}