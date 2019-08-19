SELECT ?name
WHERE 
{
  ?x ex:name ?name .
  FILTER NOT IN{"A", "B"}
}