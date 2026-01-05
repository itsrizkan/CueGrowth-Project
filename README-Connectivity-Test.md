Check Kubernestes services connectivity
---

kubectl exec -it -n cuegrowth <pod-name> -- python3 -c "
import asyncio
from nats.aio.client import Client as NATS

async def test():
    nc = NATS()
    await nc.connect('nats://cuegrowth:nats-secret-password@nats.cuegrowth.svc.cluster.local:4222')
    print('✅ connected to NATS')
    await nc.close()

asyncio.run(test())
"

---

kubectl exec -it -n cuegrowth <pod-name> -- python3 -c "
import socket
s = socket.socket()
s.settimeout(5)
s.connect(('valkey-master.cuegrowth.svc.cluster.local', 6379))
print('✅ Redis TCP connection successful')
s.close()
"


---

kubectl exec -it -n cuegrowth <pod-name> -- python3 -c "
import socket
s = socket.socket()
s.settimeout(5)
s.connect(('nats.cuegrowth.svc.cluster.local', 4222))
print('✅ NATS TCP connection successful')
s.close()
"

---
