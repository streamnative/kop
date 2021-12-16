Recently, a new serious vulnerability was reported regarding Log4j that can
allow remote execution for attackers.

The vulnerability issue is described and tracked under [CVE-2021-44228](https://nvd.nist.gov/vuln/detail/CVE-2021-44228).

# Affected KoP versions
KoP versions lower than 2.8.1.26.

# How to fix this vulnerability issue for KoP users?
You can refer to the recommended solution for this vulnerability on the [Pulsar official website](https://pulsar.apache.org/blog/2021/12/11/Log4j-CVE/).

There are 2 workarounds to patch a Pulsar deployments. You can set either of:

1. Java property: `-Dlog4j2.formatMsgNoLookups=true`
2. Environment variable: `LOG4J_FORMAT_MSG_NO_LOOKUPS=true`

In addition, if you want to upgrade the log4j version that pulsar depends on,
you also need to upgrade the log4j version that kop depends on at the same time.



