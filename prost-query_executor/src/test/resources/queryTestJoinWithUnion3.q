SELECT ?title ?author
WHERE  { { ?book <http://example.org/1/title> ?title .  ?book <http://example.org/1/author> ?author }
         UNION
         { ?book <http://example.org/2/title> ?title .  ?book <http://example.org/2/author> ?author }
       }