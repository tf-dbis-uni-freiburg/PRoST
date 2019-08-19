SELECT ?name
WHERE 
{
  ?x ex:name ?name .
  FILTER IN{"C", "D"}
}