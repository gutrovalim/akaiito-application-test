#!/bin/sh
set -e
agent=
if [ -n "$AKT_TRACER" ] && [ "$AKT_TRACER" != none ]; then
  jar="/opt/akt/agents/${AKT_TRACER}-${AKT_TRACER_VERSION}.jar"
  if [ ! -f "$jar" ]; then
    echo "entrypoint: no agent jar $jar (have: $(ls /opt/akt/agents))" >&2
    exit 2
  fi
  agent="-javaagent:$jar"
fi
exec java $agent $JAVA_OPTS -jar /opt/akt/app.jar
