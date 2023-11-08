COPY person FROM "dataset/tinysnb/vPerson.csv" (HeaDER=true, deLim=',');
COPY organisation FROM "dataset/tinysnb/vOrganisation.csv";
COPY workAt FROM "dataset/tinysnb/eWorkAt.csv";
