SELECT * 
WHERE {
        ?num <http://example.org/firstnum> ?a
        FILTER NOT EXISTS {
                ?num <http://example.org/secondnum> ?b.
                FILTER(?a = ?b)
        }
}