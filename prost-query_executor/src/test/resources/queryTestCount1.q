SELECT ?title (COUNT(?sales) AS ?count)
WHERE
{
	?book <http://example.org/title> ?title.
	?book <http://example.org/sales> ?sales.
}
GROUP BY ?title