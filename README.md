### Nacos Trace Plugin Extension

An extension plugin for Nacos that enables trace data persistence.  
It records service instance events such as registration, deregistration, heartbeat, and metadata changes,  
and persists them into a database for audit, analysis, or observability purposes.

**Key Features:**
- Hook into Nacos trace events via the official plugin extension point.  
- Store instance trace data (namespace, group, service name, IP, port, event type, timestamp, etc.) into a relational database.  
- Compatible with both Nacos standalone and cluster modes.

**How to build:**
```bash
mvn clean
```
```bash
mvn package
```

**Todo:** 
- Provide structured trace logs for later query or visualization.

This plugin can be used to build operation audit trails or service instance activity dashboards.
