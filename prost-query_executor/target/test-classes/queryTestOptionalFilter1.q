SELECT ?title ?genre ?price
WHERE
{
	?book <http://example.org/title> ?title.
	OPTIONAL {?book <http://example.org/genre> ?genre}.
	OPTIONAL {?book <http://example.org/price> ?price.FILTER(?price <= 30)}
}