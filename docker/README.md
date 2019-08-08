This directory contains docker related files.

To build and run:

   1. In KOP root dir, execute command `mvn install -DskipTests` to build KOP project.
   2. In KOP root dir, execute command `docker build -f docker/Dockerfile -t kop .` to build docker images.
   3. run docker. e.g. `docker run -it --rm kop bin/kop standalone`
