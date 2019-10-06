SELECT ?name
WHERE 
{
  ?x <http://example.org/name> ?name .
  FILTER (?x NOT IN("A", "B"))
}