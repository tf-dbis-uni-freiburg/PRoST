SELECT ?name
WHERE 
{
  ?x <http://example.org/name> ?name .
  FILTER NOT IN{"A", "B"}
}