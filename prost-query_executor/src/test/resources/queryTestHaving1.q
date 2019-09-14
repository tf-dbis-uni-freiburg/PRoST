SELECT ?title (SUM(?sales) AS ?sum)
WHERE
{
	?book <http://example.org/title> ?title.
	?book <http://example.org/sales> ?sales.
}
GROUP BY ?title
HAVING (SUM(?sales) > 5)