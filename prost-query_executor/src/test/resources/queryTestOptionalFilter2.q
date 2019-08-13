SELECT ?title ?reduced_price
WHERE
{
	?book <http://example.org/title> ?title.
	OPTIONAL {?book <http://example.org/price> ?price.FILTER(?price > 30).
			OPTIONAL {?book <http://example.org/reduced_price> ?reduced_price.FILTER(?reduced_price < 20)}
	}
}