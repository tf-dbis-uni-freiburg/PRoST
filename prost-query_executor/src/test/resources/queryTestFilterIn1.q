SELECT ?name
WHERE 
{
  ?x <http://example.org/name> ?name .
  FILTER (?x IN("C", "D"))
}