SELECT MIN(a.X), MIN(b.birthday), MIN(b.X), MIN(c.birthday), MIN(c.X), MIN(d.birthday), MIN(d.X) FROM person a, knows e1, person b, knows e2, person c, knows e3, person d, knows e4, knows e5 WHERE a.X<10000 and a.id=e1.source and e1.dest=b.id and b.id=e2.source and e2.dest=c.id and a.id=e3.source and e3.dest=d.id and d.id=e4.source and e4.dest=c.id and a.id=e5.source and e5.dest=c.id;